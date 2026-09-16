package io.quarkus.redis.lettuce.runtime.internal;

import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.jboss.logging.Logger;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;

/**
 * Factory for creating Lettuce {@link RedisClient} instances using shared {@link ClientResources}.
 * <p>
 * The client is created with externally managed {@link ClientResources} (which use Vert.x event loops).
 * The caller is responsible for shutting down the client before shutting down the {@link ClientResources}.
 */
public class LettuceConnectionFactory {

    private static final Logger LOGGER = Logger.getLogger(LettuceConnectionFactory.class);

    /**
     * Blocking command families. Their timeout is the explicit {@code Duration} argument the caller
     * passes to Redis, not the shared {@code quarkus.redis.timeout} — so the client-side command
     * timeout is disabled for them and left to the caller's own argument plus the pooled connection
     * bound (see {@code lettuce-blocking-connection-pool-proposal.md}).
     */
    private static final Set<CommandType> BLOCKING_COMMANDS = Set.of(
            CommandType.BLPOP, CommandType.BRPOP, CommandType.BLMOVE, CommandType.BLMPOP, CommandType.BRPOPLPUSH,
            CommandType.BZPOPMIN, CommandType.BZPOPMAX, CommandType.BZMPOP);

    private final RedisClient redisClient;
    private final RedisURI redisUri;

    /**
     * Creates a Lettuce {@link RedisClient} using the given shared resources, Redis URI and command timeout.
     *
     * @param clientName the Quarkus Redis client name, used for logging
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param redisUri the Redis connection URI (e.g. {@code redis://localhost:6379})
     * @param timeout the {@code quarkus.redis.timeout} applied to non-blocking commands
     */
    public LettuceConnectionFactory(String clientName, ClientResources clientResources, URI redisUri, Duration timeout) {
        RedisURI lettuceUri = RedisURI.create(redisUri);
        LOGGER.infof("Creating Lettuce RedisClient '%s' for %s:%d", clientName, lettuceUri.getHost(), lettuceUri.getPort());
        ClientOptions options = ClientOptions.builder()
                .timeoutOptions(TimeoutOptions.builder()
                        .timeoutCommands(true)
                        .timeoutSource(new NonBlockingCommandTimeoutSource(timeout))
                        .build())
                .build();
        this.redisClient = RedisClient.create(clientResources, lettuceUri);
        this.redisClient.setOptions(options);
        this.redisUri = lettuceUri;
    }

    /**
     * Creates a Lettuce {@link RedisClient} using the given shared resources, Redis URI string and command timeout.
     *
     * @param clientName the Quarkus Redis client name, used for logging
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param redisUri the Redis connection URI string (e.g. {@code redis://localhost:6379})
     * @param timeout the {@code quarkus.redis.timeout} applied to non-blocking commands
     */
    public LettuceConnectionFactory(String clientName, ClientResources clientResources, String redisUri, Duration timeout) {
        this(clientName, clientResources, URI.create(redisUri), timeout);
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

    /**
     * Applies {@code quarkus.redis.timeout} to ordinary commands so a stuck future eventually fails
     * and its connection is released, instead of being held forever. Blocking commands are exempted:
     * their timeout is the explicit {@code Duration} argument already sent to Redis, and a stuck one
     * only holds a single pooled connection rather than the shared one.
     */
    private static final class NonBlockingCommandTimeoutSource extends TimeoutOptions.TimeoutSource {

        private final long timeoutMillis;

        NonBlockingCommandTimeoutSource(Duration timeout) {
            this.timeoutMillis = timeout.toMillis();
        }

        @Override
        public long getTimeout(RedisCommand<?, ?, ?> command) {
            return command.getType() instanceof CommandType type && BLOCKING_COMMANDS.contains(type) ? -1 : timeoutMillis;
        }

    }

}
