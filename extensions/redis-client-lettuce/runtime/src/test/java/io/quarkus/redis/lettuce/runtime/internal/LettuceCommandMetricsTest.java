package io.quarkus.redis.lettuce.runtime.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.event.command.CommandFailedEvent;
import io.lettuce.core.event.command.CommandStartedEvent;
import io.lettuce.core.event.command.CommandSucceededEvent;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.RedisCommand;
import io.quarkus.redis.runtime.client.ObservableRedisMetrics;
import io.vertx.redis.client.RedisClientType;

/**
 * Verifies what {@link LettuceCommandMetrics} reports for the events Lettuce publishes for a command: one operation
 * per command, under the name of the client, with a duration measured between the two events of the command.
 */
class LettuceCommandMetricsTest {

    private record Report(String name, long durationInNs, boolean succeeded) {
    }

    private final List<Report> reports = new ArrayList<>();
    private final ObservableRedisMetrics metrics = (name, durationInNs, succeeded) -> reports
            .add(new Report(name, durationInNs, succeeded));
    private final LettuceCommandMetrics listener = new LettuceCommandMetrics("my-redis", RedisClientType.STANDALONE, metrics);

    @Test
    void reportsASucceededCommand() throws InterruptedException {
        RedisCommand<Object, Object, Object> command = ping();
        CommandStartedEvent started = new CommandStartedEvent(command, System.currentTimeMillis());
        listener.commandStarted(started);
        assertThat(reports).isEmpty();

        Thread.sleep(2);
        listener.commandSucceeded(new CommandSucceededEvent(command, started.getContext(), System.currentTimeMillis(),
                System.currentTimeMillis()));

        assertThat(reports).singleElement().satisfies(report -> {
            assertThat(report.name()).isEqualTo("my-redis");
            assertThat(report.succeeded()).isTrue();
            // measured here, not taken from the millisecond timestamps of the events (both the same above)
            assertThat(report.durationInNs()).isGreaterThanOrEqualTo(1_000_000);
        });
    }

    @Test
    void reportsAFailedCommand() {
        RedisCommand<Object, Object, Object> command = ping();
        CommandStartedEvent started = new CommandStartedEvent(command, System.currentTimeMillis());
        listener.commandStarted(started);
        listener.commandFailed(new CommandFailedEvent(command, started.getContext(),
                new RedisCommandTimeoutException("timed out")));

        assertThat(reports).singleElement().satisfies(report -> {
            assertThat(report.name()).isEqualTo("my-redis");
            assertThat(report.succeeded()).isFalse();
            assertThat(report.durationInNs()).isNotNegative();
        });
    }

    @Test
    void reportsEveryCommandOnce() {
        for (int i = 0; i < 3; i++) {
            RedisCommand<Object, Object, Object> command = ping();
            CommandStartedEvent started = new CommandStartedEvent(command, System.currentTimeMillis());
            listener.commandStarted(started);
            listener.commandSucceeded(new CommandSucceededEvent(command, started.getContext(),
                    System.currentTimeMillis(), System.currentTimeMillis()));
        }
        assertThat(reports).hasSize(3);
    }

    @Test
    void ignoresACommandItDidNotSeeStarting() {
        listener.commandSucceeded(new CommandSucceededEvent(ping(), new HashMap<>(), System.currentTimeMillis(),
                System.currentTimeMillis()));
        assertThat(reports).isEmpty();
    }

    @Test
    void recordsEveryCommandOfAStandaloneClient() {
        for (RedisCommand<Object, Object, Object> command : List.of(command(CommandType.CLUSTER, "MYID"),
                command(CommandType.INFO, "replication"), command(CommandType.ROLE), command(CommandType.SENTINEL, "masters"),
                command(CommandType.CLIENT, "SETNAME", "lettuce#ClusterTopologyRefresh"))) {
            assertThat(LettuceCommandMetrics.sentByLettuce(RedisClientType.STANDALONE, command)).isFalse();
        }
    }

    @Test
    void leavesOutWhatLettuceSendsToFollowACluster() {
        assertThat(sentByLettuce(RedisClientType.CLUSTER, command(CommandType.CLUSTER, "MYID"))).isTrue();
        assertThat(sentByLettuce(RedisClientType.CLUSTER,
                command(CommandType.CLIENT, "SETNAME", "lettuce#ClusterTopologyRefresh"))).isTrue();

        assertThat(sentByLettuce(RedisClientType.CLUSTER, command(CommandType.CLUSTER, "INFO"))).isFalse();
        assertThat(sentByLettuce(RedisClientType.CLUSTER, command(CommandType.CLIENT, "SETNAME", "mine"))).isFalse();
        assertThat(sentByLettuce(RedisClientType.CLUSTER, command(CommandType.INFO, "replication"))).isFalse();
        assertThat(sentByLettuce(RedisClientType.CLUSTER, command(CommandType.GET, "key"))).isFalse();
    }

    @Test
    void leavesOutWhatLettuceSendsToFollowAMasterAndItsReplicas() {
        assertThat(sentByLettuce(RedisClientType.REPLICATION, command(CommandType.INFO, "replication"))).isTrue();
        assertThat(sentByLettuce(RedisClientType.REPLICATION, command(CommandType.ROLE))).isTrue();
        assertThat(sentByLettuce(RedisClientType.REPLICATION,
                command(CommandType.CLIENT, "SETNAME", "lettuce#MasterReplicaTopologyRefresh"))).isTrue();
        assertThat(sentByLettuce(RedisClientType.SENTINEL, command(CommandType.SENTINEL, "REPLICAS", "mymaster"))).isTrue();
        assertThat(sentByLettuce(RedisClientType.SENTINEL,
                command(CommandType.SUBSCRIBE, "+sdown", "+switch-master", "-odown"))).isTrue();

        assertThat(sentByLettuce(RedisClientType.REPLICATION, command(CommandType.INFO, "memory"))).isFalse();
        assertThat(sentByLettuce(RedisClientType.SENTINEL, command(CommandType.SUBSCRIBE, "news"))).isFalse();
        assertThat(sentByLettuce(RedisClientType.SENTINEL, command(CommandType.INFO, "replication"))).isFalse();
        assertThat(sentByLettuce(RedisClientType.SENTINEL, command(CommandType.SET, "key", "value"))).isFalse();
    }

    @Test
    void doesNotReportWhatLettuceSends() {
        LettuceCommandMetrics cluster = new LettuceCommandMetrics("my-redis", RedisClientType.CLUSTER, metrics);
        RedisCommand<Object, Object, Object> command = command(CommandType.CLUSTER, "MYID");
        CommandStartedEvent started = new CommandStartedEvent(command, System.currentTimeMillis());
        cluster.commandStarted(started);
        cluster.commandSucceeded(new CommandSucceededEvent(command, started.getContext(), System.currentTimeMillis(),
                System.currentTimeMillis()));
        assertThat(reports).isEmpty();
    }

    private static boolean sentByLettuce(RedisClientType clientType, RedisCommand<Object, Object, Object> command) {
        return LettuceCommandMetrics.sentByLettuce(clientType, command);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static RedisCommand<Object, Object, Object> command(CommandType type, String... arguments) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        for (String argument : arguments) {
            args.add(argument);
        }
        return (RedisCommand) new Command<>(type, new StatusOutput<>(StringCodec.UTF8), args);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static RedisCommand<Object, Object, Object> ping() {
        return (RedisCommand) new Command<>(CommandType.PING, new StatusOutput<>(StringCodec.UTF8));
    }
}
