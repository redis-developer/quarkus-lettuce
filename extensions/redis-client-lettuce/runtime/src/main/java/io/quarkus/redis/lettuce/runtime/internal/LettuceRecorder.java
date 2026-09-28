package io.quarkus.redis.lettuce.runtime.internal;

import static io.quarkus.redis.runtime.client.config.RedisConfig.HOSTS;
import static io.quarkus.redis.runtime.client.config.RedisConfig.HOSTS_PROVIDER_NAME;
import static io.quarkus.redis.runtime.client.config.RedisConfig.getPropertyName;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.ActiveResult;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.lettuce.runtime.internal.datasource.LettuceBlockingRedisDataSourceImpl;
import io.quarkus.redis.lettuce.runtime.internal.datasource.LettuceReactiveRedisDataSourceImpl;
import io.quarkus.redis.runtime.client.config.NetConfig;
import io.quarkus.redis.runtime.client.config.RedisClientConfig;
import io.quarkus.redis.runtime.client.config.RedisConfig;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;
import io.quarkus.tls.TlsConfigurationRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.redis.client.RedisClientType;

/**
 * Quarkus recorder that manages the lifecycle of Lettuce Redis clients.
 * <p>
 * Creates {@link io.lettuce.core.resource.ClientResources} with shared Vert.x event loops,
 * {@link io.lettuce.core.RedisClient} instances configured from the {@code quarkus.redis[.<name>].*} properties the
 * Lettuce backend honours (see {@link LettuceClientSettings}), and {@link StatefulRedisConnection} instances. Also
 * creates, per client, a bounded {@link LettuceConnectionPool} used for blocking commands and scoped connections
 * ({@code withConnection}/{@code withTransaction}) so they never occupy the shared connection.
 * <p>
 * Shutdown ordering: connections → pools → clients → resources (before Vert.x event loops).
 */
@Recorder
public class LettuceRecorder {

    private static final Logger LOGGER = Logger.getLogger(LettuceRecorder.class);
    private static final Duration POOL_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final RuntimeValue<RedisConfig> runtimeConfig;

    private static volatile LettuceClientResources sharedResources;
    private static volatile io.vertx.mutiny.core.Vertx mutinyVertx;
    private static final Map<String, LettuceConnectionFactory> factories = new ConcurrentHashMap<>();
    private static final Map<String, StatefulRedisConnection<byte[], byte[]>> connections = new ConcurrentHashMap<>();
    private static final Map<String, StatefulRedisConnection<String, String>> stringConnections = new ConcurrentHashMap<>();
    private static final Map<String, LettuceReactiveRedisDataSourceImpl> reactiveDataSources = new ConcurrentHashMap<>();
    private static final Map<String, LettuceConnectionPool> pools = new ConcurrentHashMap<>();

    public LettuceRecorder(RuntimeValue<RedisConfig> runtimeConfig) {
        this.runtimeConfig = runtimeConfig;
    }

    /**
     * Initializes shared client resources and creates a Lettuce RedisClient for each requested client name.
     * Only creates clients that pass the {@link #checkActive(String)} check.
     */
    public void initialize(RuntimeValue<Vertx> vertx, Supplier<TlsConfigurationRegistry> tlsRegistry, Set<String> names) {
        EventLoopGroup eventLoopGroup = ((VertxInternal) vertx.getValue()).eventLoopGroup();
        sharedResources = new LettuceClientResources(eventLoopGroup);
        mutinyVertx = io.vertx.mutiny.core.Vertx.newInstance(vertx.getValue());

        for (String name : names) {
            if (checkActive(name).get().value()) {
                RedisClientConfig clientConfig = runtimeConfig.getValue().clients().get(name);
                // checkActive() guarantees at least one host for an active client
                Set<URI> hosts = clientConfig.hosts().orElseThrow();
                warnAboutUnsupportedConfiguration(name, clientConfig, hosts);
                URI redisUri = hosts.iterator().next();
                LettuceClientSettings settings = LettuceClientSettings.create(name, clientConfig, redisUri, vertx.getValue(),
                        tlsRegistry.get());
                LettuceConnectionFactory factory = factories.computeIfAbsent(name,
                        k -> new LettuceConnectionFactory(name, sharedResources.clientResources(), settings.redisUri(),
                                settings.clientOptions(), clientConfig.timeout()));
                pools.putIfAbsent(name, new LettuceConnectionPool(factory::connectAsync,
                        clientConfig.maxPoolSize(), clientConfig.maxPoolWaiting(), factory.getDatabase()));
            }
        }
    }

    private static boolean hasHosts(RedisClientConfig config) {
        return config.hosts().isPresent() && !config.hosts().get().isEmpty();
    }

    /**
     * The Lettuce backend applies the hosts (first URI), timeout, active, password, TLS,
     * {@code tcp.secure-transport-protocols}, {@code max-pool-size} and {@code max-pool-waiting} properties. Tell users
     * at startup which other configured properties are not applied, instead of silently connecting differently than
     * configured. Properties with a default value are reported only when set to something else.
     */
    private static void warnAboutUnsupportedConfiguration(String name, RedisClientConfig config, Set<URI> hosts) {
        List<String> ignored = new ArrayList<>();
        if (hosts.size() > 1) {
            ignored.add(getPropertyName(name, HOSTS) + " (only the first URI is used)");
        }
        if (config.clientType() != RedisClientType.STANDALONE) {
            ignored.add(getPropertyName(name, "client-type") + " (only standalone is supported)");
        }
        if (config.poolCleanerInterval().isPresent()) {
            ignored.add(getPropertyName(name, "pool-cleaner-interval"));
        }
        if (config.poolRecycleTimeout().isPresent() && !config.poolRecycleTimeout().get().equals(Duration.ofMinutes(3))) {
            ignored.add(getPropertyName(name, "pool-recycle-timeout"));
        }
        if (config.maxWaitingHandlers() != 2048) {
            ignored.add(getPropertyName(name, "max-waiting-handlers"));
        }
        if (config.maxNestedArrays() != 32) {
            ignored.add(getPropertyName(name, "max-nested-arrays"));
        }
        if (config.reconnectAttempts() != 0) {
            ignored.add(getPropertyName(name, "reconnect-attempts"));
        }
        if (!config.reconnectInterval().equals(Duration.ofSeconds(1))) {
            ignored.add(getPropertyName(name, "reconnect-interval"));
        }
        if (!config.protocolNegotiation()) {
            ignored.add(getPropertyName(name, "protocol-negotiation"));
        }
        if (config.preferredProtocolVersion().isPresent()) {
            ignored.add(getPropertyName(name, "preferred-protocol-version"));
        }
        if (config.clientName().isPresent()) {
            ignored.add(getPropertyName(name, "client-name"));
        }
        if (config.configureClientName()) {
            ignored.add(getPropertyName(name, "configure-client-name"));
        }
        for (String tcpProperty : configuredTcpProperties(config.tcp())) {
            ignored.add(getPropertyName(name, "tcp." + tcpProperty));
        }
        if (!ignored.isEmpty()) {
            LOGGER.warnf("Lettuce Redis client '%s': the following configuration is not applied by the Lettuce backend yet: %s",
                    name, String.join(", ", ignored));
        }
    }

    /**
     * The names of the {@code tcp.*} properties that are set, except {@code secure-transport-protocols} which is
     * applied to the TLS handshake.
     */
    private static List<String> configuredTcpProperties(NetConfig tcp) {
        Map<String, Boolean> present = new LinkedHashMap<>();
        present.put("alpn", tcp.alpn().isPresent());
        present.put("application-layer-protocols", tcp.applicationLayerProtocols().isPresent());
        present.put("idle-timeout", tcp.idleTimeout().isPresent());
        present.put("connection-timeout", tcp.connectionTimeout().isPresent());
        present.put("proxy-configuration-name", tcp.proxyConfigurationName().isPresent());
        present.put("non-proxy-hosts", tcp.nonProxyHosts().isPresent());
        present.put("read-idle-timeout", tcp.readIdleTimeout().isPresent());
        present.put("receive-buffer-size", tcp.receiveBufferSize().isPresent());
        // The defaults of the client-level reconnect-attempts (0) and reconnect-interval (1s) are registered for every
        // client as quarkus.redis.*.reconnect-attempts and quarkus.redis.*.reconnect-interval, which the tcp group of
        // the default client (quarkus.redis.tcp.*) also matches: both tcp properties are always present for that
        // client. Treat them like defaulted properties and report them only when set to something else.
        present.put("reconnect-attempts", tcp.reconnectAttempts().isPresent() && tcp.reconnectAttempts().getAsInt() != 0);
        present.put("reconnect-interval",
                tcp.reconnectInterval().isPresent() && !tcp.reconnectInterval().get().equals(Duration.ofSeconds(1)));
        present.put("reuse-address", tcp.reuseAddress().isPresent());
        present.put("reuse-port", tcp.reusePort().isPresent());
        present.put("send-buffer-size", tcp.sendBufferSize().isPresent());
        present.put("so-linger", tcp.soLinger().isPresent());
        present.put("cork", tcp.cork().isPresent());
        present.put("fast-open", tcp.fastOpen().isPresent());
        present.put("keep-alive", tcp.keepAlive().isPresent());
        present.put("no-delay", tcp.noDelay().isPresent());
        present.put("quick-ack", tcp.quickAck().isPresent());
        present.put("traffic-class", tcp.trafficClass().isPresent());
        present.put("write-idle-timeout", tcp.writeIdleTimeout().isPresent());
        present.put("local-address", tcp.localAddress().isPresent());
        List<String> configured = new ArrayList<>();
        for (Map.Entry<String, Boolean> entry : present.entrySet()) {
            if (entry.getValue()) {
                configured.add(entry.getKey());
            }
        }
        return configured;
    }

    public Supplier<Object> getClientResources() {
        return () -> sharedResources.clientResources();
    }

    public Supplier<Object> getRedisClient(String name) {
        return () -> factories.get(name).getRedisClient();
    }

    public Supplier<Object> getConnection(String name) {
        return () -> stringConnections.computeIfAbsent(name, k -> {
            LOGGER.infof("Opening StatefulRedisConnection for client '%s'", k);
            return factories.get(k).getRedisClient().connect(StringCodec.UTF8);
        });
    }

    private static StatefulRedisConnection<byte[], byte[]> dataSourceConnection(String name) {
        return connections.computeIfAbsent(name, k -> {
            LOGGER.infof("Opening data source StatefulRedisConnection for client '%s'", k);
            return factories.get(k).connect();
        });
    }

    public Supplier<ReactiveRedisDataSource> getReactiveDataSource(String name) {
        return () -> reactiveDataSources.computeIfAbsent(name, k -> {
            StatefulRedisConnection<byte[], byte[]> conn = dataSourceConnection(k);
            return new LettuceReactiveRedisDataSourceImpl(mutinyVertx, conn, pools.get(k));
        });
    }

    public Supplier<RedisDataSource> getBlockingDataSource(String name) {
        return () -> {
            Duration timeout = runtimeConfig.getValue().clients().get(name).timeout();
            return new LettuceBlockingRedisDataSourceImpl(
                    (LettuceReactiveRedisDataSourceImpl) getReactiveDataSource(name).get(), timeout);
        };
    }

    public Supplier<ActiveResult> checkActive(final String name) {
        return () -> {
            RedisClientConfig redisClientConfig = runtimeConfig.getValue().clients().get(name);
            if (!redisClientConfig.active()) {
                return ActiveResult.inactive(String.format(
                        """
                                Lettuce Redis Client '%s' was deactivated through configuration properties. \
                                To activate the Redis Client, set configuration property '%s' to 'true' and configure the Redis Client '%s'. \
                                Refer to https://quarkus.io/guides/redis-reference for guidance.
                                """,
                        name, getPropertyName(name, "active"), name));
            }
            if (!hasHosts(redisClientConfig)) {
                if (redisClientConfig.hostsProviderName().isPresent()) {
                    return ActiveResult.inactive(String.format(
                            """
                                    Lettuce Redis Client '%s' was deactivated automatically because it is configured through '%s', \
                                    which the Lettuce backend does not support yet. Set the configuration property '%s' instead. \
                                    Refer to https://quarkus.io/guides/redis-reference for guidance.
                                    """,
                            name, getPropertyName(name, HOSTS_PROVIDER_NAME), getPropertyName(name, HOSTS)));
                }
                return ActiveResult.inactive(String.format(
                        """
                                Lettuce Redis Client '%s' was deactivated automatically because the hosts are not set. \
                                To activate the Redis Client, set the configuration property '%s'. \
                                Refer to https://quarkus.io/guides/redis-reference for guidance.
                                """,
                        name, getPropertyName(name, HOSTS)));
            }
            return ActiveResult.active();
        };
    }

    public void cleanup(ShutdownContext context) {
        context.addShutdownTask(() -> {
            closeConnections(connections);
            closeConnections(stringConnections);
            reactiveDataSources.clear();

            for (Map.Entry<String, LettuceConnectionPool> entry : pools.entrySet()) {
                try {
                    entry.getValue().close().await().atMost(POOL_CLOSE_TIMEOUT);
                } catch (Exception e) {
                    LOGGER.warnf(e, "Error closing Lettuce connection pool for '%s'", entry.getKey());
                }
            }
            pools.clear();

            for (Map.Entry<String, LettuceConnectionFactory> entry : factories.entrySet()) {
                try {
                    entry.getValue().shutdown();
                } catch (Exception e) {
                    LOGGER.warnf(e, "Error shutting down Lettuce RedisClient for '%s'", entry.getKey());
                }
            }
            factories.clear();

            if (sharedResources != null) {
                sharedResources.shutdown();
                sharedResources = null;
            }
            mutinyVertx = null;
        });
    }

    private static void closeConnections(Map<String, ? extends StatefulRedisConnection<?, ?>> connectionsByClient) {
        for (Map.Entry<String, ? extends StatefulRedisConnection<?, ?>> entry : connectionsByClient.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.warnf(e, "Error closing Lettuce connection for client '%s'", entry.getKey());
            }
        }
        connectionsByClient.clear();
    }
}
