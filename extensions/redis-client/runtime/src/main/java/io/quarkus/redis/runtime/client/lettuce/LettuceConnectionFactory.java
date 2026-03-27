package io.quarkus.redis.runtime.client.lettuce;

import java.net.URI;

import org.jboss.logging.Logger;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.ClientResources;

/**
 * Factory for creating Lettuce {@link RedisClient} instances using shared {@link ClientResources}.
 * <p>
 * The client is created with externally managed {@link ClientResources} (which use Vert.x event loops).
 * The caller is responsible for shutting down the client before shutting down the {@link ClientResources}.
 */
public class LettuceConnectionFactory {

    private static final Logger LOGGER = Logger.getLogger(LettuceConnectionFactory.class);

    private final RedisClient redisClient;

    /**
     * Creates a Lettuce {@link RedisClient} using the given shared resources and Redis URI.
     *
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param redisUri the Redis connection URI (e.g. {@code redis://localhost:6379})
     */
    public LettuceConnectionFactory(ClientResources clientResources, URI redisUri) {
        RedisURI lettuceUri = RedisURI.create(redisUri);
        LOGGER.infof("Creating Lettuce RedisClient for %s:%d", lettuceUri.getHost(), lettuceUri.getPort());
        this.redisClient = RedisClient.create(clientResources, lettuceUri);
    }

    /**
     * Creates a Lettuce {@link RedisClient} using the given shared resources and Redis URI string.
     *
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param redisUri the Redis connection URI string (e.g. {@code redis://localhost:6379})
     */
    public LettuceConnectionFactory(ClientResources clientResources, String redisUri) {
        this(clientResources, URI.create(redisUri));
    }

    /**
     * Opens a new stateful connection to Redis using String codec.
     *
     * @return a new {@link StatefulRedisConnection}
     */
    public StatefulRedisConnection<String, String> connect() {
        return redisClient.connect(StringCodec.UTF8);
    }

    /**
     * Returns the underlying {@link RedisClient}.
     */
    public RedisClient getRedisClient() {
        return redisClient;
    }

    /**
     * Shuts down the Lettuce {@link RedisClient}.
     * Must be called before shutting down the shared {@link ClientResources}.
     */
    public void shutdown() {
        LOGGER.info("Shutting down Lettuce RedisClient");
        redisClient.shutdown();
    }
}
