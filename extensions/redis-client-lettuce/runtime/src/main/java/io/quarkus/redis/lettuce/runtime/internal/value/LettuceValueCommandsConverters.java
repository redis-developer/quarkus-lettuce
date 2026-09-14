package io.quarkus.redis.lettuce.runtime.internal.value;

import java.util.Set;

import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.lettuce.runtime.internal.ArgReplay;

public final class LettuceValueCommandsConverters {

    private LettuceValueCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.SetArgs toLettuceSetArgs(SetArgs quarkus) {
        return new io.lettuce.core.SetArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replayExcept(quarkus, args, Set.of("GET"));
            }
        };
    }

    public static io.lettuce.core.GetExArgs toLettuceGetExArgs(GetExArgs quarkus) {
        return new io.lettuce.core.GetExArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(quarkus, args);
            }
        };
    }

}
