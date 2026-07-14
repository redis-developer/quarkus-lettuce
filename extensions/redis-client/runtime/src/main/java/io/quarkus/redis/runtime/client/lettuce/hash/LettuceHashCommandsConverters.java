package io.quarkus.redis.runtime.client.lettuce.hash;

import java.util.List;

import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;

/**
 * Converters bridging Quarkus Hash Command argument types to their Lettuce equivalents.
 * <p>
 * Registration with {@link LettuceConverterRegistry} happens in this class's static initializer.
 */
public final class LettuceHashCommandsConverters {

    static {
        LettuceConverterRegistry.registerArgConverter(ScanArgs.class,
                LettuceHashCommandsConverters::toLettuceScanArgs);
    }

    private LettuceHashCommandsConverters() {
        // Utility class
    }

    /**
     * Ensures the Hash Command converters are registered with {@link LettuceConverterRegistry}.
     * <p>
     * The registration itself runs in this class's static initializer; calling this method simply
     * forces class initialization at a well-defined point. It is therefore idempotent and
     * thread-safe by virtue of the JVM's class-initialization guarantees.
     */
    public static void register() {
        // No-op: registration is performed in the static initializer.
    }

    /**
     * Convert a Quarkus {@link SetArgs} to a Lettuce {@link io.lettuce.core.SetArgs}.
     */
    public static io.lettuce.core.ScanArgs toLettuceScanArgs(ScanArgs scanArgs) {
        io.lettuce.core.ScanArgs lettuce = new io.lettuce.core.ScanArgs();
        List<String> args = scanArgs.toArgs();
        if (args.isEmpty()) {
            return lettuce;
        }
        if (args.getFirst().equals("COUNT")) {
            lettuce.limit(Long.parseLong(args.get(1)));
        }
        if (args.getFirst().equals("MATCH")) {
            lettuce.match(args.get(1));
        }
        if (args.size() > 2) {
            lettuce.match(args.get(3));
        }
        return lettuce;
    }
}
