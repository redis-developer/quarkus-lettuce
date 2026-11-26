package io.quarkus.redis.lettuce.runtime.internal;

import io.lettuce.core.event.command.CommandBaseEvent;
import io.lettuce.core.event.command.CommandFailedEvent;
import io.lettuce.core.event.command.CommandListener;
import io.lettuce.core.event.command.CommandStartedEvent;
import io.lettuce.core.event.command.CommandSucceededEvent;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.RedisCommand;
import io.quarkus.redis.runtime.client.ObservableRedisMetrics;
import io.vertx.redis.client.RedisClientType;

/**
 * Reports the commands of a Lettuce client to the {@link ObservableRedisMetrics} the Vert.x client reports to, so
 * that the Redis metrics are the same whatever the backend.
 * <p>
 * Lettuce notifies the listeners of a client of every command written to one of its connections: the shared one, the
 * pooled ones and the Pub/Sub ones alike. Every command counts as one operation, the ones queued in a transaction
 * included, as do {@code MULTI} and {@code EXEC}; the Vert.x client reports a batch as one operation. A command
 * fails when the server answers with an error, when it times out or is cancelled, and when its connection is lost
 * before the reply. Lettuce completes a command before it notifies the listeners, so a command is recorded a moment
 * after its caller got the result.
 * <p>
 * The commands of the handshake ({@code HELLO}, {@code AUTH}, {@code CLIENT SETNAME}, {@code CLIENT SETINFO},
 * {@code SELECT}) are written to the channel directly and never get here. The ones Lettuce sends on its own to
 * discover and follow the topology of a cluster, Sentinel or replication client do, a cluster client sending some
 * to every node every {@code topology-cache-ttl}: they are not the application's, and are left out (see
 * {@link #sentByLettuce}). A standalone client sends none.
 */
final class LettuceCommandMetrics implements CommandListener {

    /**
     * The key of the start time in the context of a command's events. The events carry the wall clock in
     * milliseconds, too coarse for commands that take microseconds.
     */
    private static final String STARTED_NANOS = LettuceCommandMetrics.class.getName() + ".started";

    /** The class of the commands of Lettuce's topology refresh, of a cluster and of a master/replica client alike. */
    private static final String TOPOLOGY_REFRESH_COMMAND = "TimedAsyncCommand";
    /** How Lettuce names the connections of its topology refresh: {@code CLIENT SETNAME lettuce#...}. */
    private static final String TOPOLOGY_REFRESH_CONNECTION_NAME = "lettuce#";
    /** One of the Sentinel events Lettuce subscribes to on the sentinels, to follow a failover. */
    private static final String SENTINEL_EVENT = "+switch-master";

    private final String clientName;
    private final RedisClientType clientType;
    private final ObservableRedisMetrics metrics;

    LettuceCommandMetrics(String clientName, RedisClientType clientType, ObservableRedisMetrics metrics) {
        this.clientName = clientName;
        this.clientType = clientType;
        this.metrics = metrics;
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        if (!sentByLettuce(clientType, event.getCommand())) {
            event.getContext().put(STARTED_NANOS, System.nanoTime());
        }
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        report(event, true);
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        report(event, false);
    }

    private void report(CommandBaseEvent event, boolean succeeded) {
        if (event.getContext().get(STARTED_NANOS) instanceof Long started) {
            metrics.report(clientName, System.nanoTime() - started, succeeded);
        }
    }

    /**
     * Whether a command is one Lettuce sends on its own, for a client of the given type:
     * <ul>
     * <li>a cluster client: {@code CLUSTER NODES} and {@code INFO} to every node, on connections it names with
     * {@code CLIENT SETNAME}, when it discovers the topology and every time it refreshes it; {@code CLUSTER MYID}
     * when a cluster connection is opened;</li>
     * <li>a Sentinel client: {@code SENTINEL} commands and the subscription to the Sentinel events, on the
     * sentinels; {@code PING} to the nodes, on connections it names;</li>
     * <li>a replication client: {@code INFO replication} or, with {@code topology=static}, {@code ROLE}, and
     * {@code PING}, to the nodes, on connections it names.</li>
     * </ul>
     * The listener is not told which connection a command is written to, so these are recognised by what they are:
     * the same command sent by the application to such a client is left out too. The tests of the metrics of each
     * client type fail when a Lettuce version sends something else.
     */
    static boolean sentByLettuce(RedisClientType clientType, RedisCommand<?, ?, ?> command) {
        if (clientType == RedisClientType.STANDALONE) {
            return false;
        }
        if (TOPOLOGY_REFRESH_COMMAND.equals(command.getClass().getSimpleName())) {
            return true;
        }
        if (!(command.getType() instanceof CommandType type)) {
            return false;
        }
        return switch (type) {
            case CLIENT -> arguments(command).contains(TOPOLOGY_REFRESH_CONNECTION_NAME);
            case CLUSTER -> clientType == RedisClientType.CLUSTER && arguments(command).equals("MYID");
            case INFO -> clientType == RedisClientType.REPLICATION && arguments(command).equals("replication");
            case ROLE -> clientType == RedisClientType.REPLICATION;
            case SENTINEL -> clientType == RedisClientType.SENTINEL;
            case SUBSCRIBE -> clientType == RedisClientType.SENTINEL && arguments(command).contains(SENTINEL_EVENT);
            default -> false;
        };
    }

    private static String arguments(RedisCommand<?, ?, ?> command) {
        CommandArgs<?, ?> args = command.getArgs();
        return args == null ? "" : args.toCommandString();
    }
}
