package io.quarkus.redis.runtime.client.lettuce.hash;

import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.runtime.client.lettuce.ArgTokenCursor;

/**
 * Converters bridging Quarkus Hash Command argument types to their Lettuce equivalents.
 */
public final class LettuceHashCommandsConverters {

    private LettuceHashCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.ScanArgs toLettuceScanArgs(ScanArgs quarkus) {
        io.lettuce.core.ScanArgs lettuce = new io.lettuce.core.ScanArgs();
        Iterable<String> tokens = quarkus.toArgs();
        var cursor = new ArgTokenCursor(tokens);
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "MATCH" -> lettuce.match(cursor.next());
                case "COUNT" -> lettuce.limit(cursor.nextLong(token));
                default -> throw new IllegalStateException("Unexpected ScanArgs token: " + token);
            }
        }
        return lettuce;
    }

}
