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
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.protocol.TransactionalCommand;
import io.lettuce.core.resource.ClientResources;

/**
 * Factory for the connections of one Quarkus Redis client, backed by a Lettuce {@link RedisClient} (standalone,
 * Sentinel and replication) or {@link RedisClusterClient} (cluster) created with shared {@link ClientResources}.
 * <p>
 * The connections it opens are {@link LettuceConnection}s, the same handle whatever the topology, so the data
 * sources and the {@link LettuceConnectionPool} do not depend on the client type. The client type decides the
 * {@link Topology}: a cluster client discovers the topology from the configured seed nodes, refreshes it
 * periodically and on {@code MOVED}/{@code ASK} redirects and reconnects (see
 * {@link ClusterTopologyRefreshOptions}), and reads from the nodes selected by its {@link ReadFrom}; a Sentinel or
 * replication client opens master/replica connections (see {@link io.lettuce.core.masterreplica.MasterReplica})
 * that write to the master and read from the nodes selected by their {@link ReadFrom}.
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

    private final Topology topology;

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
        RedisClient client = RedisClient.create(clientResources, redisUri);
        client.setOptions(withCommandTimeout(clientOptions, timeout));
        this.topology = new Standalone(client, redisUri);
    }

    /**
     * Creates a Lettuce {@link RedisClusterClient} using the given shared resources, seed nodes, client options,
     * topology refresh options, read preference and command timeout.
     * <p>
     * Lettuce connects to the nodes it discovers with the settings of the first seed URI (credentials, TLS), so all
     * the seeds are expected to carry the same ones, as {@link LettuceClientSettings} produces them. A cluster only
     * has database {@code 0}: the database of the seed URIs is ignored.
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
        RedisClusterClient client = RedisClusterClient.create(clientResources, seeds);
        client.setOptions(ClusterClientOptions.builder(withCommandTimeout(clientOptions, timeout))
                .topologyRefreshOptions(topologyRefresh)
                .build());
        this.topology = new Cluster(client, readFrom);
    }

    /**
     * Creates a Lettuce {@link RedisClient} whose connections are master/replica connections built by
     * {@link io.lettuce.core.masterreplica.MasterReplica}:
     * <ul>
     * <li>{@code nodes} is a single URI with sentinels (see {@link LettuceClientSettings#sentinelUri}): the
     * connections follow the master the sentinels monitor, and the replicas they report; Lettuce subscribes to the
     * sentinel events, so a failover is followed without any reconnection by the caller;</li>
     * <li>{@code nodes} is a single Redis URI: the master and its replicas are discovered from that node
     * ({@code INFO replication}) when a connection is opened;</li>
     * <li>{@code nodes} are several Redis URIs: they are the nodes, each asked for its role when a connection is
     * opened.</li>
     * </ul>
     * The last two keep the topology they discovered for the life of the connection. All write to the master and
     * read from the nodes {@code readFrom} selects.
     *
     * @param clientName the Quarkus Redis client name, used for logging
     * @param clientResources shared client resources (with Vert.x event loops)
     * @param nodes the sentinel URI, or the Redis URI(s), as described above
     * @param clientOptions the client options, carrying the TLS material (see {@link LettuceClientSettings}); the
     *        command timeout options are added to them
     * @param readFrom the nodes read-only commands are sent to (see {@link LettuceClientSettings#readFrom})
     * @param timeout the {@code quarkus.redis.timeout} applied to non-blocking commands
     */
    public LettuceConnectionFactory(String clientName, ClientResources clientResources, List<RedisURI> nodes,
            ClientOptions clientOptions, ReadFrom readFrom, Duration timeout) {
        nonNull(nodes, "nodes");
        nonNull(readFrom, "readFrom");
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("At least one node is required for the Redis client " + clientName);
        }
        RedisURI first = nodes.get(0);
        if (nodes.size() == 1 && !first.getSentinels().isEmpty()) {
            LOGGER.infof(
                    "Creating Lettuce RedisClient '%s' for the master '%s' monitored by the sentinels %s%s, reading from %s",
                    clientName, first.getSentinelMasterId(), describe(first.getSentinels()), first.isSsl() ? " (TLS)" : "",
                    describe(readFrom));
        } else {
            LOGGER.infof("Creating Lettuce RedisClient '%s' for the replication nodes %s%s, reading from %s", clientName,
                    describe(nodes), first.isSsl() ? " (TLS)" : "", describe(readFrom));
        }
        RedisClient client = RedisClient.create(clientResources);
        client.setOptions(withCommandTimeout(clientOptions, timeout));
        this.topology = new MasterReplica(client, List.copyOf(nodes), readFrom);
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

    private static String describe(List<RedisURI> nodes) {
        List<String> hosts = new ArrayList<>(nodes.size());
        for (RedisURI node : nodes) {
            hosts.add(node.getHost() + ":" + node.getPort());
        }
        return String.join(", ", hosts);
    }

    /**
     * Opens a new connection using the {@link ByteArrayCodec} codec: to the server, to the master and its replicas,
     * or to the cluster.
     *
     * @return a new {@link LettuceConnection}
     */
    public LettuceConnection connect() {
        return topology.connect();
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
        return topology.connectAsync();
    }

    /**
     * Opens a new connection using the given codec, as the Lettuce connection beans do: a
     * {@link io.lettuce.core.api.StatefulRedisConnection} to a standalone server, a
     * {@link StatefulRedisClusterConnection} to a cluster, a {@link StatefulRedisMasterReplicaConnection} to a
     * Sentinel-managed or replicated master.
     */
    public <K, V> StatefulConnection<K, V> connect(RedisCodec<K, V> codec) {
        return topology.connect(codec);
    }

    /**
     * Returns the database every connection this factory opens starts on: the database index of the configured URI
     * ({@code 0} unless the URI names one) for a standalone server and for a master/replica connection (the one of
     * the first node), always {@code 0} for a cluster, which has no other database (Lettuce ignores the database of
     * the seed URIs; the recorder warns about it).
     */
    public int getDatabase() {
        return topology.database();
    }

    /**
     * Whether this factory connects to a cluster.
     */
    public boolean isCluster() {
        return topology instanceof Cluster;
    }

    /**
     * Whether this factory opens master/replica connections (Sentinel or replication client).
     */
    public boolean isMasterReplica() {
        return topology instanceof MasterReplica;
    }

    /**
     * Returns the underlying Lettuce client: a {@link RedisClient} or a {@link RedisClusterClient}.
     */
    public AbstractRedisClient getClient() {
        return topology.client();
    }

    /**
     * Returns the underlying {@link RedisClient}: the one of a standalone server, or the one opening the
     * master/replica connections of a Sentinel or replication client.
     *
     * @throws IllegalStateException if this factory connects to a cluster
     */
    public RedisClient getRedisClient() {
        if (topology instanceof Standalone standalone) {
            return standalone.client();
        }
        if (topology instanceof MasterReplica masterReplica) {
            return masterReplica.client();
        }
        throw new IllegalStateException("This Lettuce client connects to a cluster, use getClusterClient()");
    }

    /**
     * Returns the underlying {@link RedisClusterClient}.
     *
     * @throws IllegalStateException if this factory connects to a standalone server
     */
    public RedisClusterClient getClusterClient() {
        if (topology instanceof Cluster cluster) {
            return cluster.client();
        }
        throw new IllegalStateException("This Lettuce client connects to a standalone server, use getRedisClient()");
    }

    /**
     * Shuts down the Lettuce client.
     * Must be called before shutting down the shared {@link ClientResources}.
     */
    public void shutdown() {
        LOGGER.infof("Shutting down Lettuce %s", topology.client().getClass().getSimpleName());
        topology.client().shutdown();
    }

    /**
     * How the connections of a client are opened, one implementation per client type: the Lettuce client, the
     * connections it opens with the byte-array codec for the data sources and the pool, and with a given codec for
     * the Lettuce beans, and the database every connection starts on.
     */
    private sealed interface Topology permits Standalone, Cluster, MasterReplica {

        AbstractRedisClient client();

        LettuceConnection connect();

        CompletionStage<LettuceConnection> connectAsync();

        <K, V> StatefulConnection<K, V> connect(RedisCodec<K, V> codec);

        int database();

    }

    /**
     * A standalone server: plain connections to {@code uri}, starting on its database.
     */
    private record Standalone(RedisClient client, RedisURI uri) implements Topology {

        @Override
        public LettuceConnection connect() {
            return LettuceConnection.standalone(client.connect(ByteArrayCodec.INSTANCE));
        }

        @Override
        public CompletionStage<LettuceConnection> connectAsync() {
            return client.connectAsync(ByteArrayCodec.INSTANCE, uri).thenApply(LettuceConnection::standalone);
        }

        @Override
        public <K, V> StatefulConnection<K, V> connect(RedisCodec<K, V> codec) {
            return client.connect(codec);
        }

        @Override
        public int database() {
            return uri.getDatabase();
        }

    }

    /**
     * A cluster: connections routing each command to the node owning its key and reading from the nodes
     * {@code readFrom} selects. A cluster only has database {@code 0}.
     */
    private record Cluster(RedisClusterClient client, ReadFrom readFrom) implements Topology {

        @Override
        public LettuceConnection connect() {
            StatefulRedisClusterConnection<byte[], byte[]> connection = client.connect(ByteArrayCodec.INSTANCE);
            connection.setReadFrom(readFrom);
            return LettuceConnection.cluster(connection);
        }

        @Override
        public CompletionStage<LettuceConnection> connectAsync() {
            return client.connectAsync(ByteArrayCodec.INSTANCE).thenApply(connection -> {
                connection.setReadFrom(readFrom);
                return LettuceConnection.cluster(connection);
            });
        }

        @Override
        public <K, V> StatefulConnection<K, V> connect(RedisCodec<K, V> codec) {
            StatefulRedisClusterConnection<K, V> connection = client.connect(codec);
            connection.setReadFrom(readFrom);
            return connection;
        }

        @Override
        public int database() {
            return 0;
        }

    }

    /**
     * A master and its replicas, managed by Redis Sentinel or addressed directly: master/replica connections built by
     * Lettuce's {@code io.lettuce.core.masterreplica.MasterReplica} (named in full, as this record takes its name)
     * from a single sentinel URI, a single Redis URI to discover the topology from, or the Redis URIs of the nodes,
     * writing to the master and reading from the nodes {@code readFrom} selects. The connections are
     * {@link io.lettuce.core.api.StatefulRedisConnection}s, so they take the standalone path of
     * {@link LettuceConnection}. The database is the one of the first node.
     */
    private record MasterReplica(RedisClient client, List<RedisURI> nodes, ReadFrom readFrom) implements Topology {

        @Override
        public LettuceConnection connect() {
            return LettuceConnection.standalone(connectMasterReplica(ByteArrayCodec.INSTANCE));
        }

        @Override
        public CompletionStage<LettuceConnection> connectAsync() {
            return connectMasterReplicaAsync(ByteArrayCodec.INSTANCE).thenApply(LettuceConnection::standalone);
        }

        @Override
        public <K, V> StatefulConnection<K, V> connect(RedisCodec<K, V> codec) {
            return connectMasterReplica(codec);
        }

        @Override
        public int database() {
            return nodes.get(0).getDatabase();
        }

        private <K, V> StatefulRedisMasterReplicaConnection<K, V> connectMasterReplica(RedisCodec<K, V> codec) {
            StatefulRedisMasterReplicaConnection<K, V> connection = nodes.size() == 1
                    ? io.lettuce.core.masterreplica.MasterReplica.connect(client, codec, nodes.get(0))
                    : io.lettuce.core.masterreplica.MasterReplica.connect(client, codec, nodes);
            connection.setReadFrom(readFrom);
            return connection;
        }

        private <K, V> CompletionStage<StatefulRedisMasterReplicaConnection<K, V>> connectMasterReplicaAsync(
                RedisCodec<K, V> codec) {
            CompletionStage<StatefulRedisMasterReplicaConnection<K, V>> connecting = nodes.size() == 1
                    ? io.lettuce.core.masterreplica.MasterReplica.connectAsync(client, codec, nodes.get(0))
                    : io.lettuce.core.masterreplica.MasterReplica.connectAsync(client, codec, nodes);
            return connecting.thenApply(connection -> {
                connection.setReadFrom(readFrom);
                return connection;
            });
        }

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
