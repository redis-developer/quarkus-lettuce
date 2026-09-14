package io.quarkus.redis.lettuce.runtime.internal.set;

import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.lettuce.runtime.internal.ArgReplay;

public final class LettuceSetCommandsConverters {

    private LettuceSetCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.ScanArgs toLettuceScanArgs(ScanArgs quarkus) {
        return new io.lettuce.core.ScanArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(quarkus.toArgs(), args);
            }
        };
    }

    public static io.lettuce.core.SortArgs toLettuceSortArgs(SortArgs quarkus) {
        return new io.lettuce.core.SortArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(quarkus, args);
            }
        };
    }

}
