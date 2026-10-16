package io.quarkus.redis.lettuce.runtime.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;

class BlockingReadsTest {

    @Test
    void aMarkIsConsumedByTheFirstUnmark() {
        CommandArgs<String, String> args = args();
        BlockingReads.mark(args);
        assertThat(BlockingReads.unmark(args)).isTrue();
        assertThat(BlockingReads.unmark(args)).isFalse();
    }

    @Test
    void marksArePerInstanceNotPerContent() {
        // two reads with identical arguments are two commands: consuming one mark must leave the other
        CommandArgs<String, String> first = args();
        CommandArgs<String, String> second = args();
        BlockingReads.mark(first);
        BlockingReads.mark(second);
        assertThat(BlockingReads.unmark(first)).isTrue();
        assertThat(BlockingReads.unmark(second)).isTrue();
        assertThat(BlockingReads.unmark(second)).isFalse();
    }

    @Test
    void unmarkedAndNullArgumentsAreNotBlocking() {
        assertThat(BlockingReads.unmark(args())).isFalse();
        assertThat(BlockingReads.unmark(null)).isFalse();
    }

    private static CommandArgs<String, String> args() {
        return new CommandArgs<>(StringCodec.UTF8).add("BLOCK").add("1000").add("STREAMS").addKey("k").add("$");
    }

}
