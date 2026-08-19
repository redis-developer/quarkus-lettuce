package io.quarkus.redis.runtime.client.lettuce.sortedset;

import java.util.ArrayList;
import java.util.List;

import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.sortedset.Range;
import io.quarkus.redis.datasource.sortedset.ScoreRange;
import io.quarkus.redis.datasource.sortedset.ZAddArgs;
import io.quarkus.redis.datasource.sortedset.ZAggregateArgs;
import io.quarkus.redis.runtime.client.lettuce.ArgTokenCursor;

/**
 * Converters bridging Quarkus Sorted Set Command argument types to their Lettuce equivalents.
 */
public final class LettuceSortedSetCommandsConverters {

    private LettuceSortedSetCommandsConverters() {
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
                    if (cursor.isNextNumeric()) {
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

    public static io.lettuce.core.ZAddArgs toLettuceZAddArgs(ZAddArgs quarkus) {
        io.lettuce.core.ZAddArgs lettuce = new io.lettuce.core.ZAddArgs();
        for (Object raw : quarkus.toArgs()) {
            String token = raw.toString();
            switch (token) {
                case "NX" -> lettuce.nx();
                case "XX" -> lettuce.xx();
                case "LT" -> lettuce.lt();
                case "GT" -> lettuce.gt();
                case "CH" -> lettuce.ch();
                default -> throw new IllegalStateException("Unexpected ZAddArgs token: " + token);
            }
        }
        return lettuce;
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
        return apply(quarkus, new io.lettuce.core.ZAggregateArgs());
    }

    public static io.lettuce.core.ZStoreArgs toLettuceZStoreArgs(ZAggregateArgs quarkus) {
        return apply(quarkus, new io.lettuce.core.ZStoreArgs());
    }

    private static <T extends io.lettuce.core.ZAggregateArgs> T apply(ZAggregateArgs quarkus, T lettuce) {
        List<Double> weights = new ArrayList<>();
        String aggregate = null;
        Iterable<Object> tokens = quarkus.toArgs();
        var cursor = new ArgTokenCursor(tokens);
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "WEIGHTS" -> {
                    while (cursor.nextIsNumeric()) {
                        weights.add(cursor.nextDouble(token));
                    }
                    if (weights.isEmpty()) {
                        throw new IllegalStateException("Missing value for token: " + token);
                    }
                }
                case "AGGREGATE" -> aggregate = cursor.nextValue(token);
                default -> throw new IllegalStateException("Unexpected ZAggregateArgs token: " + token);
            }
        }

        if (!weights.isEmpty()) {
            double[] newWeights = new double[weights.size()];
            for (int i = 0; i < newWeights.length; i++) {
                newWeights[i] = weights.get(i);
            }
            lettuce.weights(newWeights);
        }

        if (aggregate != null) {
            switch (aggregate) {
                case "SUM" -> lettuce.sum();
                case "MIN" -> lettuce.min();
                case "MAX" -> lettuce.max();
                default -> throw new IllegalStateException("Unexpected aggregate function: " + aggregate);
            }
        }

        return lettuce;
    }

    public static io.lettuce.core.Range<String> toLettuceLexRange(Range<String> range) {
        io.lettuce.core.Range.Boundary<String> lower = toLexBoundary(range.getLowerBound());
        io.lettuce.core.Range.Boundary<String> upper = toLexBoundary(range.getUpperBound());
        return io.lettuce.core.Range.from(lower, upper);
    }

    private static io.lettuce.core.Range.Boundary<String> toLexBoundary(String bound) {
        if ("-".equals(bound) || "+".equals(bound)) {
            return io.lettuce.core.Range.Boundary.unbounded();
        }
        if (bound.startsWith("(")) {
            return io.lettuce.core.Range.Boundary.excluding(bound.substring(1));
        }
        if (bound.startsWith("[")) {
            return io.lettuce.core.Range.Boundary.including(bound.substring(1));
        }
        throw new IllegalStateException("Unexpected lexicographical boundary: " + bound);
    }

}
