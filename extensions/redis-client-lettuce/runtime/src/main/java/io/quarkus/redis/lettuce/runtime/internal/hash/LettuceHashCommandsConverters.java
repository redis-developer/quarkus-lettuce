package io.quarkus.redis.lettuce.runtime.internal.hash;

import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.lettuce.runtime.internal.ArgReplay;

public final class LettuceHashCommandsConverters {

    private LettuceHashCommandsConverters() {
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

}
