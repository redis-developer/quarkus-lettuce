package io.quarkus.redis.runtime.client.lettuce;

import java.net.URI;
import java.util.concurrent.CompletionStage;

import org.jboss.logging.Logger;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
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
    private final RedisURI redisUri;

    /**
     * Creates a Lettuce {@link RedisClient} using the given shared resources and Redis URI.
     *
     * @param clientName the Quarkus Redis client name, used for logging
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param redisUri the Redis connection URI (e.g. {@code redis://localhost:6379})
     */
    public LettuceConnectionFactory(String clientName, ClientResources clientResources, URI redisUri) {
        RedisURI lettuceUri = RedisURI.create(redisUri);
        LOGGER.infof("Creating Lettuce RedisClient '%s' for %s:%d", clientName, lettuceUri.getHost(), lettuceUri.getPort());
        this.redisClient = RedisClient.create(clientResources, lettuceUri);
        this.redisUri = lettuceUri;
    }

    /**
     * Creates a Lettuce {@link RedisClient} using the given shared resources and Redis URI string.
     *
     * @param clientName the Quarkus Redis client name, used for logging
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param redisUri the Redis connection URI string (e.g. {@code redis://localhost:6379})
     */
    public LettuceConnectionFactory(String clientName, ClientResources clientResources, String redisUri) {
        this(clientName, clientResources, URI.create(redisUri));
    }

    /**
     * Opens a new stateful connection to Redis using {@link ByteArrayCodec} codec.
     *
     * @return a new {@link StatefulRedisConnection}
     */
    public StatefulRedisConnection<byte[], byte[]> connect() {
        return redisClient.connect(ByteArrayCodec.INSTANCE);
    }

    /**
     * Opens a new stateful connection to Redis asynchronously using the byte-array codec.
     * <p>
     * Unlike {@link #connect()}, this never blocks the calling thread and is therefore safe to
     * invoke from an event loop; the returned stage completes once the connection is established.
     *
     * @return a {@link CompletionStage} completing with a new {@link StatefulRedisConnection}
     */
    public CompletionStage<StatefulRedisConnection<byte[], byte[]>> connectAsync() {
        return redisClient.connectAsync(ByteArrayCodec.INSTANCE, redisUri);
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
