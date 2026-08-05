package io.quarkus.redis.runtime.client.lettuce.set;

import java.util.List;

import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;

/**
 * Converters bridging Quarkus Set Command argument types to their Lettuce equivalents.
 */
public final class LettuceSetCommandsConverters {

    private LettuceSetCommandsConverters() {
        // Utility class
    }

    /**
     * Convert a Quarkus {@link ScanArgs} to a Lettuce {@link io.lettuce.core.ScanArgs}.
     */
    public static io.lettuce.core.ScanArgs toLettuceScanArgs(ScanArgs quarkus) {
        io.lettuce.core.ScanArgs lettuce = new io.lettuce.core.ScanArgs();
        List<String> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            switch (token) {
                case "MATCH" -> lettuce.match(nextToken(tokens, ++i, token));
                case "COUNT" -> lettuce.limit(nextLong(tokens, ++i, token));
                default -> throw new IllegalStateException("Unexpected ScanArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Convert a Quarkus {@link SortArgs} to a Lettuce {@link io.lettuce.core.SortArgs}.
     */
    public static io.lettuce.core.SortArgs toLettuceSortArgs(SortArgs quarkus) {
        io.lettuce.core.SortArgs lettuce = new io.lettuce.core.SortArgs();
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i).toString();
            switch (token) {
                case "BY" -> lettuce.by(nextToken(tokens, ++i, token));
                case "GET" -> lettuce.get(nextToken(tokens, ++i, token));
                case "ASC" -> lettuce.asc();
                case "DESC" -> lettuce.desc();
                case "ALPHA" -> lettuce.alpha();
                case "LIMIT" -> {
                    long first = nextLong(tokens, ++i, token);
                    // SortArgs.Limit emits a single value — the count — when its offset is -1. Only
                    // keywords can follow LIMIT, so a numeric next token can only be the count.
                    if (i + 1 < tokens.size() && isNumeric(tokens.get(i + 1).toString())) {
                        lettuce.limit(first, nextLong(tokens, ++i, token));
                    } else {
                        lettuce.limit(0, first);
                    }
                }
                default -> throw new IllegalStateException("Unexpected SortArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Return the value token at {@code index} (e.g. the pattern after {@code MATCH}).
     */
    private static String nextToken(List<?> tokens, int index, String token) {
        if (index >= tokens.size()) {
            throw new IllegalStateException("Missing value for token: " + token);
        }
        return tokens.get(index).toString();
    }

    /**
     * Return and parse the numeric value token at {@code index} (e.g. the count after {@code COUNT}).
     */
    private static long nextLong(List<?> tokens, int index, String token) {
        return Long.parseLong(nextToken(tokens, index, token));
    }

    private static boolean isNumeric(String token) {
        try {
            Long.parseLong(token);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
