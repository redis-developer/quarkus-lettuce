package io.quarkus.redis.runtime.client.lettuce.hash;

import java.util.Iterator;

import io.quarkus.redis.datasource.ScanArgs;

/**
 * Converters bridging Quarkus Hash Command argument types to their Lettuce equivalents.
 */
public final class LettuceHashCommandsConverters {

    private LettuceHashCommandsConverters() {
        // Utility class
    }

    /**
     * Convert a Quarkus {@link ScanArgs} to a Lettuce {@link io.lettuce.core.ScanArgs}.
     */
    public static io.lettuce.core.ScanArgs toLettuceScanArgs(ScanArgs quarkus) {
        io.lettuce.core.ScanArgs lettuce = new io.lettuce.core.ScanArgs();
        Iterator<String> tokens = quarkus.toArgs().iterator();
        while (tokens.hasNext()) {
            String token = tokens.next();
            switch (token) {
                case "MATCH" -> lettuce.match(nextToken(tokens, token));
                case "COUNT" -> lettuce.limit(nextLong(tokens, token));
                default -> throw new IllegalStateException("Unexpected ScanArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Consume and return the value token that follows a keyword (e.g. the pattern after {@code MATCH}).
     */
    private static String nextToken(Iterator<?> tokens, String token) {
        if (!tokens.hasNext()) {
            throw new IllegalStateException("Missing value for token: " + token);
        }
        return tokens.next().toString();
    }

    /**
     * Consume and parse the numeric value token that follows a keyword (e.g. the count after {@code COUNT}).
     */
    private static long nextLong(Iterator<?> tokens, String token) {
        return Long.parseLong(nextToken(tokens, token));
    }
}
