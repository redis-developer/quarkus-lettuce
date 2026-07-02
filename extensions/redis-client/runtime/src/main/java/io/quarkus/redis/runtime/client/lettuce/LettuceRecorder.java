package io.quarkus.redis.runtime.client.lettuce;

import static io.quarkus.redis.runtime.client.config.RedisConfig.HOSTS;
import static io.quarkus.redis.runtime.client.config.RedisConfig.HOSTS_PROVIDER_NAME;
import static io.quarkus.redis.runtime.client.config.RedisConfig.getPropertyName;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.ActiveResult;
import io.quarkus.redis.runtime.client.config.RedisClientConfig;
import io.quarkus.redis.runtime.client.config.RedisConfig;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;

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
    private static final Map<String, LettuceConnectionFactory> factories = new ConcurrentHashMap<>();
    private static final Map<String, StatefulRedisConnection<String, String>> connections = new ConcurrentHashMap<>();

    public LettuceRecorder(RuntimeValue<RedisConfig> runtimeConfig) {
        this.runtimeConfig = runtimeConfig;
    }

    /**
     * Initializes shared client resources and creates a Lettuce RedisClient for each requested client name.
     * Only creates clients that pass the {@link #checkActive(String)} check.
     */
    public void initialize(RuntimeValue<Vertx> vertx, Set<String> names) {
        EventLoopGroup eventLoopGroup = ((VertxInternal) vertx.getValue()).getEventLoopGroup();
        sharedResources = new LettuceClientResources(eventLoopGroup);

        for (String name : names) {
            if (checkActive(name).get().value()) {
                RedisClientConfig clientConfig = runtimeConfig.getValue().clients().get(name);
                Optional<Set<URI>> hosts = clientConfig.hosts();
                if (hosts.isEmpty() || hosts.get().isEmpty()) {
                    LOGGER.warnf("No hosts configured for Lettuce Redis client '%s' — skipping", name);
                    continue;
                }
                String redisUri = hosts.get().iterator().next().toString();

                LOGGER.infof("Creating Lettuce RedisClient '%s' for %s", name, redisUri);
                factories.putIfAbsent(name, new LettuceConnectionFactory(sharedResources.clientResources(), redisUri));
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
        return () -> connections.computeIfAbsent(name, k -> {
            LOGGER.infof("Opening StatefulRedisConnection for client '%s'", k);
            return factories.get(k).connect();
        });
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
            for (Map.Entry<String, StatefulRedisConnection<String, String>> entry : connections.entrySet()) {
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    LOGGER.warnf(e, "Error closing Lettuce connection for client '%s'", entry.getKey());
                }
            }
            connections.clear();

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
        });
    }
}
