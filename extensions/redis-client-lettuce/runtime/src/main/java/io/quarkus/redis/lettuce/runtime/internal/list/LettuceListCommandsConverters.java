package io.quarkus.redis.lettuce.runtime.internal.list;

import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.LPosArgs;
import io.quarkus.redis.datasource.list.Position;
import io.quarkus.redis.lettuce.runtime.internal.ArgReplay;

public final class LettuceListCommandsConverters {

    private LettuceListCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.LPosArgs toLettuceLPosArgs(LPosArgs quarkus) {
        return new io.lettuce.core.LPosArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(quarkus, args);
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
