package io.quarkus.redis.runtime.client.lettuce.list;

import java.util.List;

import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.LPosArgs;
import io.quarkus.redis.datasource.list.Position;

/**
 * Converters bridging Quarkus List Command argument types to their Lettuce equivalents.
 * <p>
 * The Quarkus argument classes expose no getters, so these parse their wire-format token list
 * ({@code toArgs()}) — the stable public contract. {@link Position} is a plain enum, so the
 * {@code LMOVE} / {@code LMPOP} converters switch on it instead. Called directly from
 * {@link LettuceReactiveListCommandsImpl}, not registered with
 * {@link io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry}.
 */
public final class LettuceListCommandsConverters {

    private LettuceListCommandsConverters() {
        // Utility class
    }

    /**
     * Convert a Quarkus {@link LPosArgs} to a Lettuce {@link io.lettuce.core.LPosArgs}.
     */
    public static io.lettuce.core.LPosArgs toLettuceLPosArgs(LPosArgs quarkus) {
        io.lettuce.core.LPosArgs lettuce = new io.lettuce.core.LPosArgs();
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i).toString();
            switch (token) {
                case "RANK" -> lettuce.rank(nextLong(tokens, ++i, token));
                case "MAXLEN" -> lettuce.maxlen(nextLong(tokens, ++i, token));
                default -> throw new IllegalStateException("Unexpected LPosArgs token: " + token);
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
     * The Lettuce {@link io.lettuce.core.LMoveArgs} for an {@code LMOVE} / {@code BLMOVE} position pair.
     */
    public static io.lettuce.core.LMoveArgs toLettuceLMoveArgs(Position positionInSource, Position positionInDest) {
        return switch (positionInSource) {
            case LEFT -> positionInDest == Position.LEFT
                    ? io.lettuce.core.LMoveArgs.Builder.leftLeft()
                    : io.lettuce.core.LMoveArgs.Builder.leftRight();
            case RIGHT -> positionInDest == Position.LEFT
                    ? io.lettuce.core.LMoveArgs.Builder.rightLeft()
                    : io.lettuce.core.LMoveArgs.Builder.rightRight();
        };
    }

    /**
     * The Lettuce {@link io.lettuce.core.LMPopArgs} for an {@code LMPOP} / {@code BLMPOP} position.
     */
    public static io.lettuce.core.LMPopArgs toLettuceLMPopArgs(Position position) {
        return switch (position) {
            case LEFT -> io.lettuce.core.LMPopArgs.Builder.left();
            case RIGHT -> io.lettuce.core.LMPopArgs.Builder.right();
        };
    }

    /**
     * As above, with a {@code COUNT}.
     */
    public static io.lettuce.core.LMPopArgs toLettuceLMPopArgs(Position position, long count) {
        return toLettuceLMPopArgs(position).count(count);
    }

    /**
     * Return the value token at {@code index} (e.g. the pattern after {@code BY}).
     */
    private static String nextToken(List<Object> tokens, int index, String token) {
        if (index >= tokens.size()) {
            throw new IllegalStateException("Missing value for token: " + token);
        }
        return tokens.get(index).toString();
    }

    /**
     * Return and parse the numeric value token at {@code index} (e.g. the rank after {@code RANK}).
     */
    private static long nextLong(List<Object> tokens, int index, String token) {
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
