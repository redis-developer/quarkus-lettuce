package io.quarkus.redis.lettuce.runtime.internal.sortedset;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.sortedset.Range;
import io.quarkus.redis.datasource.sortedset.ScoreRange;
import io.quarkus.redis.datasource.sortedset.ZAddArgs;
import io.quarkus.redis.datasource.sortedset.ZAggregateArgs;
import io.quarkus.redis.lettuce.runtime.internal.ArgReplay;

public final class LettuceSortedSetCommandsConverters {

    private LettuceSortedSetCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.ZAddArgs toLettuceZAddArgs(ZAddArgs quarkus) {
        return new io.lettuce.core.ZAddArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(quarkus, args);
            }
        };
    }

    public static io.lettuce.core.Range<Number> toLettuceScoreRange(ScoreRange<Double> range) {
        io.lettuce.core.Range.Boundary<Number> lower = toScoreBoundary(range.getLowerBound());
        io.lettuce.core.Range.Boundary<Number> upper = toScoreBoundary(range.getUpperBound());
        return io.lettuce.core.Range.from(lower, upper);
    }

    private static io.lettuce.core.Range.Boundary<Number> toScoreBoundary(String bound) {
        if ("-inf".equals(bound)) {
            return io.lettuce.core.Range.Boundary.including(Double.NEGATIVE_INFINITY);
        }
        if ("+inf".equals(bound)) {
            return io.lettuce.core.Range.Boundary.including(Double.POSITIVE_INFINITY);
        }
        if (bound.startsWith("(")) {
            return io.lettuce.core.Range.Boundary.excluding(Double.parseDouble(bound.substring(1)));
        }
        return io.lettuce.core.Range.Boundary.including(Double.parseDouble(bound));
    }

    public static io.lettuce.core.ZAggregateArgs toLettuceZAggregateArgs(ZAggregateArgs quarkus) {
        return new io.lettuce.core.ZAggregateArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(quarkus, args);
            }
        };
    }

    public static io.lettuce.core.ZStoreArgs toLettuceZStoreArgs(ZAggregateArgs quarkus) {
        return new io.lettuce.core.ZStoreArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(quarkus, args);
            }
        };
    }

    public static io.lettuce.core.Range<byte[]> toLettuceLexRange(Range<String> range) {
        io.lettuce.core.Range.Boundary<byte[]> lower = toLexBoundary(range.getLowerBound());
        io.lettuce.core.Range.Boundary<byte[]> upper = toLexBoundary(range.getUpperBound());
        return io.lettuce.core.Range.from(lower, upper);
    }

    private static io.lettuce.core.Range.Boundary<byte[]> toLexBoundary(String bound) {
        if ("-".equals(bound) || "+".equals(bound)) {
            return io.lettuce.core.Range.Boundary.unbounded();
        }
        if (bound.startsWith("(")) {
            return io.lettuce.core.Range.Boundary.excluding(bound.substring(1).getBytes(StandardCharsets.UTF_8));
        }
        if (bound.startsWith("[")) {
            return io.lettuce.core.Range.Boundary.including(bound.substring(1).getBytes(StandardCharsets.UTF_8));
        }
        throw new IllegalStateException("Unexpected lexicographical boundary: " + bound);
    }

}
