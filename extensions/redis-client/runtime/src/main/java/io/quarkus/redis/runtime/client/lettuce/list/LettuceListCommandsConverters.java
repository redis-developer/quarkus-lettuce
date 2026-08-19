package io.quarkus.redis.runtime.client.lettuce.list;

import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.LPosArgs;
import io.quarkus.redis.datasource.list.Position;
import io.quarkus.redis.runtime.client.lettuce.ArgTokenCursor;

/**
 * Converters bridging Quarkus List Command argument types to their Lettuce equivalents.
 */
public final class LettuceListCommandsConverters {

    private LettuceListCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.LPosArgs toLettuceLPosArgs(LPosArgs quarkus) {
        io.lettuce.core.LPosArgs lettuce = new io.lettuce.core.LPosArgs();
        Iterable<Object> tokens = quarkus.toArgs();
        var cursor = new ArgTokenCursor(tokens);
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "RANK" -> lettuce.rank(cursor.nextLong(token));
                case "MAXLEN" -> lettuce.maxlen(cursor.nextLong(token));
                default -> throw new IllegalStateException("Unexpected LPosArgs token: " + token);
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

    public static io.lettuce.core.LMPopArgs toLettuceLMPopArgs(Position position) {
        return switch (position) {
            case LEFT -> io.lettuce.core.LMPopArgs.Builder.left();
            case RIGHT -> io.lettuce.core.LMPopArgs.Builder.right();
        };
    }

    public static io.lettuce.core.LMPopArgs toLettuceLMPopArgs(Position position, long count) {
        return toLettuceLMPopArgs(position).count(count);
    }

}
