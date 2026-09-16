package io.quarkus.redis.lettuce.runtime.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.RedisCommandExtraArguments;

class ArgReplayTest {

    @Test
    void replayPreservesTokenOrderAndTypes() {
        CommandArgs<String, String> args = newArgs();
        ArgReplay.replay(extraArgs("LIMIT", 0L, 3L, "ALPHA"), args);
        assertThat(tokens(args)).containsExactly("LIMIT", "0", "3", "ALPHA");
    }

    @Test
    void replayOfEmptyArgumentsAddsNothing() {
        CommandArgs<String, String> args = newArgs();
        ArgReplay.replay(extraArgs(), args);
        assertThat(tokens(args)).isEmpty();
    }

    @Test
    void replayAppendsAfterExistingTokens() {
        CommandArgs<String, String> args = newArgs().addKey("k");
        ArgReplay.replay(List.of("EX", 10L), args);
        assertThat(tokens(args)).containsExactly("key<k>", "EX", "10");
    }

    @Test
    void replayExceptSkipsEveryOccurrenceOfTheListedKeywords() {
        CommandArgs<String, String> args = newArgs();
        ArgReplay.replayExcept(extraArgs("GET", "EX", 10L, "GET", "NX"), args, Set.of("GET"));
        assertThat(tokens(args)).containsExactly("EX", "10", "NX");
    }

    @Test
    void replayExceptOnlySkipsExactStringMatches() {
        CommandArgs<String, String> args = newArgs();
        ArgReplay.replayExcept(extraArgs("get", "COUNT", 10L), args, Set.of("GET", "10"));
        assertThat(tokens(args)).containsExactly("get", "COUNT", "10");
    }

    // ------------------------------------------------------------------ helpers

    private static CommandArgs<String, String> newArgs() {
        return new CommandArgs<>(StringCodec.UTF8);
    }

    /** Extra arguments emitting exactly {@code tokens}. */
    private static RedisCommandExtraArguments extraArgs(Object... tokens) {
        return new RedisCommandExtraArguments() {
            @Override
            public List<Object> toArgs() {
                return List.of(tokens);
            }
        };
    }

    private static String[] tokens(CommandArgs<String, String> args) {
        // CommandArgs.toCommandString() renders tokens space-separated, unquoted
        String rendered = args.toCommandString();
        if (rendered == null || rendered.isEmpty()) {
            return new String[0];
        }
        return rendered.split(" ");
    }
}
