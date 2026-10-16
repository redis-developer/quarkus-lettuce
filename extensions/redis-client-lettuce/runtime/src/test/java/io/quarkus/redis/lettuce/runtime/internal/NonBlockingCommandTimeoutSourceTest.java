package io.quarkus.redis.lettuce.runtime.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.TransactionalCommand;
import io.quarkus.redis.datasource.stream.XReadArgs;
import io.quarkus.redis.datasource.stream.XReadGroupArgs;
import io.quarkus.redis.lettuce.runtime.internal.LettuceConnectionFactory.NonBlockingCommandTimeoutSource;
import io.quarkus.redis.lettuce.runtime.internal.stream.LettuceStreamCommandsConverters;

class NonBlockingCommandTimeoutSourceTest {

    private static final long TIMEOUT = 500;

    private final NonBlockingCommandTimeoutSource source = new NonBlockingCommandTimeoutSource(Duration.ofMillis(TIMEOUT));

    @Test
    void ordinaryCommandsAreTimed() {
        assertThat(source.getTimeout(command(CommandType.GET, args().addKey(bytes("k"))))).isEqualTo(TIMEOUT);
        assertThat(source.getTimeout(command(CommandType.PING, args()))).isEqualTo(TIMEOUT);
    }

    @Test
    void blockingCommandsAreNotTimed() {
        assertThat(source.getTimeout(command(CommandType.BLPOP, args().addKey(bytes("k")).add("5")))).isEqualTo(-1);
        assertThat(source.getTimeout(command(CommandType.BZPOPMIN, args().add("5").addKey(bytes("k"))))).isEqualTo(-1);
    }

    @Test
    void transactionalCommandsAreNotTimed() {
        Command<byte[], byte[], String> get = command(CommandType.GET, args().addKey(bytes("k")));
        assertThat(source.getTimeout(new TransactionalCommand<>(get))).isEqualTo(-1);
    }

    @Test
    void xreadWithoutBlockIsTimed() {
        // the plain form runs on the shared connection and must keep the client-side timeout
        assertThat(source.getTimeout(command(CommandType.XREAD, xread(new XReadArgs().count(10))))).isEqualTo(TIMEOUT);
        assertThat(source.getTimeout(command(CommandType.XREADGROUP, xreadgroup(new XReadGroupArgs().noack()))))
                .isEqualTo(TIMEOUT);
    }

    @Test
    void xreadWithBlockIsNotTimed() {
        assertThat(source.getTimeout(command(CommandType.XREAD, xread(new XReadArgs().block(Duration.ofSeconds(1))))))
                .isEqualTo(-1);
        assertThat(source.getTimeout(
                command(CommandType.XREADGROUP, xreadgroup(new XReadGroupArgs().count(2).block(Duration.ZERO)))))
                .isEqualTo(-1);
    }

    @Test
    void theBlockMarkIsConsumedByTheFirstLookup() {
        Command<byte[], byte[], String> read = command(CommandType.XREAD, xread(new XReadArgs().block(Duration.ofSeconds(1))));
        assertThat(source.getTimeout(read)).isEqualTo(-1);
        assertThat(source.getTimeout(read)).isEqualTo(TIMEOUT);
        assertThat(BlockingReads.unmark(read.getArgs())).isFalse();
    }

    @Test
    void aBlockMarkDoesNotLeakOntoOtherCommandTypes() {
        CommandArgs<byte[], byte[]> args = xread(new XReadArgs().block(Duration.ofSeconds(1)));
        assertThat(source.getTimeout(command(CommandType.XRANGE, args))).isEqualTo(TIMEOUT);
        assertThat(BlockingReads.unmark(args)).isTrue();
    }

    /**
     * The arguments as {@code RedisCommandBuilder.xread} builds them: the converted options, then the streams.
     */
    private static CommandArgs<byte[], byte[]> xread(XReadArgs quarkus) {
        CommandArgs<byte[], byte[]> args = args();
        LettuceStreamCommandsConverters.toXReadArgs(quarkus).build(args);
        return args.add("STREAMS").addKey(bytes("k")).add("0-0");
    }

    private static CommandArgs<byte[], byte[]> xreadgroup(XReadGroupArgs quarkus) {
        CommandArgs<byte[], byte[]> args = args().add("GROUP").add(bytes("g1")).add(bytes("c1"));
        LettuceStreamCommandsConverters.toXReadArgs(quarkus).build(args);
        return args.add("STREAMS").addKey(bytes("k")).add(">");
    }

    private static CommandArgs<byte[], byte[]> args() {
        return new CommandArgs<>(ByteArrayCodec.INSTANCE);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Command<byte[], byte[], String> command(CommandType type, CommandArgs<byte[], byte[]> args) {
        return new Command<>(type, new StatusOutput<>(ByteArrayCodec.INSTANCE), args);
    }

}
