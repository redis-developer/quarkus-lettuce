package io.quarkus.redis.lettuce.runtime.internal;

import static io.quarkus.redis.runtime.client.config.RedisConfig.HOSTS;
import static io.quarkus.redis.runtime.client.config.RedisConfig.HOSTS_PROVIDER_NAME;
import static io.quarkus.redis.runtime.client.config.RedisConfig.getPropertyName;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
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
import io.quarkus.redis.runtime.client.config.RedisClientConfig;
import io.quarkus.redis.runtime.client.config.RedisConfig;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;

/**
 * Quarkus recorder that manages the lifecycle of Lettuce Redis clients.
 * <p>
 * Creates {@link io.lettuce.core.resource.ClientResources} with shared Vert.x event loops,
 * {@link io.lettuce.core.RedisClient} instances configured from {@code quarkus.redis.hosts},
 * and {@link StatefulRedisConnection} instances.
 * <p>
 * Shutdown ordering: connections → clients → resources (before Vert.x event loops).
 */
@Recorder
public class LettuceRecorder {

    private static final Logger LOGGER = Logger.getLogger(LettuceRecorder.class);

    private final RuntimeValue<RedisConfig> runtimeConfig;

    private static volatile LettuceClientResources sharedResources;
    private static volatile io.vertx.mutiny.core.Vertx mutinyVertx;
    private static final Map<String, LettuceConnectionFactory> factories = new ConcurrentHashMap<>();
    private static final Map<String, StatefulRedisConnection<byte[], byte[]>> connections = new ConcurrentHashMap<>();
    private static final Map<String, StatefulRedisConnection<String, String>> stringConnections = new ConcurrentHashMap<>();
    private static final Map<String, LettuceReactiveRedisDataSourceImpl> reactiveDataSources = new ConcurrentHashMap<>();

    public LettuceRecorder(RuntimeValue<RedisConfig> runtimeConfig) {
        this.runtimeConfig = runtimeConfig;
    }

    /**
     * Initializes shared client resources and creates a Lettuce RedisClient for each requested client name.
     * Only creates clients that pass the {@link #checkActive(String)} check.
     */
    public void initialize(RuntimeValue<Vertx> vertx, Set<String> names) {
        EventLoopGroup eventLoopGroup = ((VertxInternal) vertx.getValue()).eventLoopGroup();
        sharedResources = new LettuceClientResources(eventLoopGroup);
        mutinyVertx = io.vertx.mutiny.core.Vertx.newInstance(vertx.getValue());

        for (String name : names) {
            if (checkActive(name).get().value()) {
                RedisClientConfig clientConfig = runtimeConfig.getValue().clients().get(name);
                Optional<Set<URI>> hosts = clientConfig.hosts();
                if (hosts.isEmpty() || hosts.get().isEmpty()) {
                    LOGGER.warnf("No hosts configured for Lettuce Redis client '%s' — skipping", name);
                    continue;
                }
                URI redisUri = hosts.get().iterator().next();
                factories.putIfAbsent(name, new LettuceConnectionFactory(name, sharedResources.clientResources(), redisUri));
            }
        }
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
            LettuceConnectionFactory factory = factories.get(k);
            return new LettuceReactiveRedisDataSourceImpl(mutinyVertx, conn, factory::connectAsync);
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
            if (redisClientConfig.hosts().isEmpty() && redisClientConfig.hostsProviderName().isEmpty()) {
                return ActiveResult.inactive(String.format(
                        """
                                Lettuce Redis Client '%s' was deactivated automatically because neither the hosts nor the hostsProviderName is set. \
                                To activate the Redis Client, set the configuration property '%s' or '%s'. \
                                Refer to https://quarkus.io/guides/redis-reference for guidance.
                                """,
                        name, getPropertyName(name, HOSTS), getPropertyName(name, HOSTS_PROVIDER_NAME)));
            }
            return ActiveResult.active();
        };
    }

    public void cleanup(ShutdownContext context) {
        context.addShutdownTask(() -> {
            closeConnections(connections);
            closeConnections(stringConnections);
            reactiveDataSources.clear();

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
