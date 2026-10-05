package io.quarkus.redis.lettuce.runtime.internal;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.jboss.logging.Logger;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.protocol.TransactionalCommand;
import io.lettuce.core.resource.ClientResources;

/**
 * Factory for the connections of one Quarkus Redis client, backed by a Lettuce {@link RedisClient} (standalone) or
 * {@link RedisClusterClient} (cluster) created with shared {@link ClientResources}.
 * <p>
 * The connections it opens are {@link LettuceConnection}s, the same handle whatever the topology, so the data
 * sources and the {@link LettuceConnectionPool} do not depend on the client type. A cluster client discovers the
 * topology from the configured seed nodes, refreshes it periodically and on {@code MOVED}/{@code ASK} redirects and
 * reconnects (see {@link ClusterTopologyRefreshOptions}), and reads from the nodes selected by its {@link ReadFrom}.
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

    private final AbstractRedisClient client;
    /** The standalone client, {@code null} for a cluster. */
    private final RedisClient redisClient;
    /** The cluster client, {@code null} for a standalone server. */
    private final RedisClusterClient clusterClient;
    /** The URI of the standalone server, or the first seed node of the cluster. */
    private final RedisURI redisUri;
    /** The nodes a cluster connection reads from, {@code null} for a standalone server. */
    private final ReadFrom readFrom;

    /**
     * Creates a Lettuce {@link RedisClient} using the given shared resources, Redis URI, client options and command
     * timeout.
     *
     * @param clientName the Quarkus Redis client name, used for logging
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param redisUri the Redis URI, carrying the host, the credentials and the TLS mode
     * @param clientOptions the client options, carrying the TLS material (see {@link LettuceClientSettings}); the
     *        command timeout options are added to them
     * @param timeout the {@code quarkus.redis.timeout} applied to non-blocking commands
     */
    public LettuceConnectionFactory(String clientName, ClientResources clientResources, RedisURI redisUri,
            ClientOptions clientOptions, Duration timeout) {
        LOGGER.infof("Creating Lettuce RedisClient '%s' for %s:%d%s", clientName, redisUri.getHost(), redisUri.getPort(),
                redisUri.isSsl() ? " (TLS)" : "");
        this.redisClient = RedisClient.create(clientResources, redisUri);
        this.redisClient.setOptions(withCommandTimeout(clientOptions, timeout));
        this.clusterClient = null;
        this.client = redisClient;
        this.redisUri = redisUri;
        this.readFrom = null;
    }

    /**
     * Creates a Lettuce {@link RedisClusterClient} using the given shared resources, seed nodes, client options,
     * topology refresh options, read preference and command timeout.
     * <p>
     * Lettuce connects to the nodes it discovers with the settings of the first seed URI (credentials, TLS,
     * database), so all the seeds are expected to carry the same ones, as {@link LettuceClientSettings} produces
     * them.
     *
     * @param clientName the Quarkus Redis client name, used for logging
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param seeds the URIs of the cluster nodes to discover the topology from; need not be all the nodes
     * @param clientOptions the client options, carrying the TLS material (see {@link LettuceClientSettings}); the
     *        command timeout options and the topology refresh options are added to them
     * @param topologyRefresh when the cluster topology is refreshed (see
     *        {@link LettuceClientSettings#topologyRefreshOptions})
     * @param readFrom the nodes read-only commands are sent to (see {@link LettuceClientSettings#readFrom})
     * @param timeout the {@code quarkus.redis.timeout} applied to non-blocking commands
     */
    public LettuceConnectionFactory(String clientName, ClientResources clientResources, List<RedisURI> seeds,
            ClientOptions clientOptions, ClusterTopologyRefreshOptions topologyRefresh, ReadFrom readFrom,
            Duration timeout) {
        nonNull(seeds, "seeds");
        nonNull(topologyRefresh, "topologyRefresh");
        nonNull(readFrom, "readFrom");
        if (seeds.isEmpty()) {
            throw new IllegalArgumentException("At least one seed node is required for the Redis cluster client "
                    + clientName);
        }
        LOGGER.infof("Creating Lettuce RedisClusterClient '%s' for the seed nodes %s%s, reading from %s", clientName,
                describe(seeds), seeds.get(0).isSsl() ? " (TLS)" : "", describe(readFrom));
        this.clusterClient = RedisClusterClient.create(clientResources, seeds);
        this.clusterClient.setOptions(ClusterClientOptions.builder(withCommandTimeout(clientOptions, timeout))
                .topologyRefreshOptions(topologyRefresh)
                .build());
        this.redisClient = null;
        this.client = clusterClient;
        this.redisUri = seeds.get(0);
        this.readFrom = readFrom;
    }

    /**
     * Creates a Lettuce {@link RedisClient} using the given shared resources, Redis URI and command timeout, with the
     * default client options. TLS and credentials, if any, must be encoded in the URI.
     *
     * @param clientName the Quarkus Redis client name, used for logging
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param redisUri the Redis connection URI (e.g. {@code redis://localhost:6379})
     * @param timeout the {@code quarkus.redis.timeout} applied to non-blocking commands
     */
    public LettuceConnectionFactory(String clientName, ClientResources clientResources, URI redisUri, Duration timeout) {
        this(clientName, clientResources, RedisURI.create(redisUri), ClientOptions.create(), timeout);
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

    private static ClientOptions withCommandTimeout(ClientOptions clientOptions, Duration timeout) {
        return clientOptions.mutate()
                .timeoutOptions(TimeoutOptions.builder()
                        .timeoutCommands(true)
                        .timeoutSource(new NonBlockingCommandTimeoutSource(timeout))
                        .build())
                .build();
    }

    /**
     * Names the read setting for the logs: the {@link ReadFrom} constants do not override {@code toString()}.
     */
    private static String describe(ReadFrom readFrom) {
        if (readFrom == ReadFrom.UPSTREAM) {
            return "the upstream nodes";
        }
        if (readFrom == ReadFrom.ANY) {
            return "any node";
        }
        if (readFrom == ReadFrom.REPLICA_PREFERRED) {
            return "the replicas, else the upstream node";
        }
        if (readFrom == ReadFrom.REPLICA) {
            return "the replicas";
        }
        return readFrom.getClass().getSimpleName();
    }

    private static String describe(List<RedisURI> seeds) {
        List<String> hosts = new ArrayList<>(seeds.size());
        for (RedisURI seed : seeds) {
            hosts.add(seed.getHost() + ":" + seed.getPort());
        }
        return String.join(", ", hosts);
    }

    /**
     * Opens a new connection using the {@link ByteArrayCodec} codec: to the server, or to the cluster.
     *
     * @return a new {@link LettuceConnection}
     */
    public LettuceConnection connect() {
        if (clusterClient != null) {
            StatefulRedisClusterConnection<byte[], byte[]> connection = clusterClient.connect(ByteArrayCodec.INSTANCE);
            connection.setReadFrom(readFrom);
            return LettuceConnection.cluster(connection);
        }
        return LettuceConnection.standalone(redisClient.connect(ByteArrayCodec.INSTANCE));
    }

    /**
     * Opens a new connection asynchronously using the byte-array codec.
     * <p>
     * Unlike {@link #connect()}, this never blocks the calling thread and is therefore safe to
     * invoke from an event loop; the returned stage completes once the connection is established.
     *
     * @return a {@link CompletionStage} completing with a new {@link LettuceConnection}
     */
    public CompletionStage<LettuceConnection> connectAsync() {
        if (clusterClient != null) {
            return clusterClient.connectAsync(ByteArrayCodec.INSTANCE).thenApply(connection -> {
                connection.setReadFrom(readFrom);
                return LettuceConnection.cluster(connection);
            });
        }
        return redisClient.connectAsync(ByteArrayCodec.INSTANCE, redisUri).thenApply(LettuceConnection::standalone);
    }

    /**
     * Opens a new connection using the given codec, as the Lettuce connection beans do: a
     * {@link io.lettuce.core.api.StatefulRedisConnection} to a standalone server, a
     * {@link StatefulRedisClusterConnection} to a cluster.
     */
    public <K, V> StatefulConnection<K, V> connect(RedisCodec<K, V> codec) {
        if (clusterClient != null) {
            StatefulRedisClusterConnection<K, V> connection = clusterClient.connect(codec);
            connection.setReadFrom(readFrom);
            return connection;
        }
        return redisClient.connect(codec);
    }

    /**
     * Returns the database index of the configured URI ({@code 0} unless the URI names one).
     * Every connection this factory opens starts on it. A cluster only has database {@code 0}.
     */
    public int getDatabase() {
        return redisUri.getDatabase();
    }

    /**
     * Whether this factory connects to a cluster.
     */
    public boolean isCluster() {
        return clusterClient != null;
    }

    /**
     * Returns the underlying Lettuce client: a {@link RedisClient} or a {@link RedisClusterClient}.
     */
    public AbstractRedisClient getClient() {
        return client;
    }

    /**
     * Returns the underlying {@link RedisClient}.
     *
     * @throws IllegalStateException if this factory connects to a cluster
     */
    public RedisClient getRedisClient() {
        if (redisClient == null) {
            throw new IllegalStateException("This Lettuce client connects to a cluster, use getClusterClient()");
        }
        return redisClient;
    }

    /**
     * Returns the underlying {@link RedisClusterClient}.
     *
     * @throws IllegalStateException if this factory connects to a standalone server
     */
    public RedisClusterClient getClusterClient() {
        if (clusterClient == null) {
            throw new IllegalStateException("This Lettuce client connects to a standalone server, use getRedisClient()");
        }
        return clusterClient;
    }

    /**
     * Shuts down the Lettuce client.
     * Must be called before shutting down the shared {@link ClientResources}.
     */
    public void shutdown() {
        LOGGER.infof("Shutting down Lettuce %s", client.getClass().getSimpleName());
        client.shutdown();
    }

    /**
     * Applies {@code quarkus.redis.timeout} to ordinary commands so a stuck future eventually fails
     * and its connection is released, instead of being held forever. Two kinds are exempted:
     * <ul>
     * <li>Blocking commands: their timeout is the explicit {@code Duration} argument already sent to
     * Redis, and a stuck one only holds a single pooled connection rather than the shared one.</li>
     * <li>Commands queued between {@code MULTI} and {@code EXEC}: Redis answers {@code QUEUED} at once,
     * but Lettuce completes their futures only with the {@code EXEC} reply, so a timer started when they
     * are written would fail commands the transaction then went on to execute. {@code EXEC} itself is
     * timed and bounds them all.</li>
     * </ul>
     */
    private static final class NonBlockingCommandTimeoutSource extends TimeoutOptions.TimeoutSource {

        private final long timeoutMillis;

        NonBlockingCommandTimeoutSource(Duration timeout) {
            this.timeoutMillis = timeout.toMillis();
        }

        @Override
        public long getTimeout(RedisCommand<?, ?, ?> command) {
            if (command instanceof TransactionalCommand<?, ?, ?>) {
                return -1; // completes on EXEC, bounded by the EXEC timeout
            }
            return command.getType() instanceof CommandType type && BLOCKING_COMMANDS.contains(type) ? -1 : timeoutMillis;
        }

    }

}
