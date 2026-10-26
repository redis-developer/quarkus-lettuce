package io.quarkus.redis.lettuce.runtime.internal;

import java.util.List;
import java.util.Set;

import io.lettuce.core.protocol.CommandArgs;

public final class ArgReplay {

    private ArgReplay() {
        // Utility class
    }

    public static <K, V> void replay(List<?> tokens, CommandArgs<K, V> args) {
        for (Object token : tokens) {
            if (token != null) {
                args.add(token);
            }
        }
    }

    public static <K, V> void replayExcept(List<?> tokens, CommandArgs<K, V> args, Set<String> skip) {
        for (Object token : tokens) {
            if (token != null && !(token instanceof String s && skip.contains(s))) {
                args.add(token);
            }
        }
    }

}
