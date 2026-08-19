package io.quarkus.redis.runtime.client.lettuce.set;

import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.runtime.client.lettuce.ArgTokenCursor;

/**
 * Converters bridging Quarkus Set Command argument types to their Lettuce equivalents.
 */
public final class LettuceSetCommandsConverters {

    private LettuceSetCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.ScanArgs toLettuceScanArgs(ScanArgs quarkus) {
        io.lettuce.core.ScanArgs lettuce = new io.lettuce.core.ScanArgs();
        Iterable<String> tokens = quarkus.toArgs();
        var cursor = new ArgTokenCursor(tokens);
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "MATCH" -> lettuce.match(cursor.nextValue(token));
                case "COUNT" -> lettuce.limit(cursor.nextLong(token));
                default -> throw new IllegalStateException("Unexpected ScanArgs token: " + token);
            }
        }
        return lettuce;
    }

    public static io.lettuce.core.SortArgs toLettuceSortArgs(SortArgs quarkus) {
        io.lettuce.core.SortArgs lettuce = new io.lettuce.core.SortArgs();
        Iterable<Object> tokens = quarkus.toArgs();
        var cursor = new ArgTokenCursor(tokens);
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "BY" -> lettuce.by(cursor.nextValue(token));
                case "GET" -> lettuce.get(cursor.nextValue(token));
                case "ASC" -> lettuce.asc();
                case "DESC" -> lettuce.desc();
                case "ALPHA" -> lettuce.alpha();
                case "LIMIT" -> {
                    long first = cursor.nextLong(token);
                    if (cursor.nextIsNumeric()) {
                        lettuce.limit(first, cursor.nextLong(token));
                    } else {
                        lettuce.limit(0, first);
                    }
                }
                default -> throw new IllegalStateException("Unexpected SortArgs token: " + token);
            }
        }
        return lettuce;
    }

}
