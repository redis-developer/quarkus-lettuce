package io.quarkus.redis.runtime.client.lettuce.sortedset;

import java.util.ArrayList;
import java.util.List;

import io.lettuce.core.Limit;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.sortedset.Range;
import io.quarkus.redis.datasource.sortedset.ScoreRange;
import io.quarkus.redis.datasource.sortedset.ZAddArgs;
import io.quarkus.redis.datasource.sortedset.ZAggregateArgs;
import io.quarkus.redis.datasource.sortedset.ZRangeArgs;

/**
 * Converters bridging Quarkus Sorted Set Command argument types to their Lettuce equivalents.
 */
public final class LettuceSortedSetCommandsConverters {

    /**
     * A range no member can fall into: nothing sorts below the empty string, so an upper boundary
     * excluding it matches no member. See {@link #toLettuceLexRange}.
     */
    private static final io.lettuce.core.Range<String> EMPTY_LEX_RANGE = io.lettuce.core.Range.from(
            io.lettuce.core.Range.Boundary.excluding(""), io.lettuce.core.Range.Boundary.excluding(""));

    private LettuceSortedSetCommandsConverters() {
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
     * Convert a Quarkus {@link ZAddArgs} to a Lettuce {@link io.lettuce.core.ZAddArgs}.
     */
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

    /**
     * Convert a Quarkus {@link ZAggregateArgs} to a Lettuce {@link io.lettuce.core.ZAggregateArgs}.
     */
    public static io.lettuce.core.ZAggregateArgs toLettuceZAggregateArgs(ZAggregateArgs quarkus) {
        return apply(parseAggregation(quarkus), new io.lettuce.core.ZAggregateArgs());
    }

    /**
     * Convert a Quarkus {@link ZAggregateArgs} to a Lettuce {@link io.lettuce.core.ZStoreArgs}.
     */
    public static io.lettuce.core.ZStoreArgs toLettuceZStoreArgs(ZAggregateArgs quarkus) {
        return apply(parseAggregation(quarkus), new io.lettuce.core.ZStoreArgs());
    }

    /**
     * Split a Quarkus {@link ZRangeArgs} into the two things Lettuce expresses separately.
     */
    public static RangeOptions toLettuceRangeOptions(ZRangeArgs quarkus) {
        boolean reverse = false;
        Limit limit = Limit.unlimited();
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i).toString();
            switch (token) {
                case "REV" -> reverse = true;
                case "LIMIT" -> {
                    long offset = nextLong(tokens, ++i, token);
                    limit = Limit.create(offset, nextLong(tokens, ++i, token));
                }
                default -> throw new IllegalStateException("Unexpected ZRangeArgs token: " + token);
            }
        }
        return new RangeOptions(reverse, limit);
    }

    /**
     * Convert a Quarkus {@link ScoreRange} to a Lettuce {@link io.lettuce.core.Range} of scores.
     * <p>
     * When {@code swapped} is set the two boundaries are exchanged. Lettuce's {@code REV} commands emit
     * the upper boundary first, whereas the Vert.x backend emits the boundaries in declaration order and
     * therefore expects a reversed query to carry {@code (max, min)}; swapping restores that contract.
     * An infinite boundary is carried as an {@link InfiniteScore} rather than
     * {@link io.lettuce.core.Range.Boundary#unbounded()}: Lettuce renders an unbounded boundary by the
     * slot it occupies (lower slot {@code -inf}, upper slot {@code +inf}), which would flip the sign of
     * a swapped infinity.
     *
     * @param range the Quarkus score range
     * @param swapped whether to exchange the two boundaries
     * @return the Lettuce range
     */
    public static io.lettuce.core.Range<Number> toLettuceScoreRange(ScoreRange<Double> range, boolean swapped) {
        io.lettuce.core.Range.Boundary<Number> lower = toScoreBoundary(range.getLowerBound(), InfiniteScore.NEGATIVE);
        io.lettuce.core.Range.Boundary<Number> upper = toScoreBoundary(range.getUpperBound(), InfiniteScore.POSITIVE);
        return swapped ? io.lettuce.core.Range.from(upper, lower) : io.lettuce.core.Range.from(lower, upper);
    }

    /**
     * Convert a Quarkus lexicographical {@link Range} to a Lettuce {@link io.lettuce.core.Range}.
     * <p>
     * See {@link #toLettuceScoreRange} for the meaning of {@code swapped}. The returned range carries
     * {@code String} boundaries, which Lettuce encodes with the connection codec's <em>value</em> codec —
     * see {@link LettuceReactiveSortedSetCommandsImpl} for why that restricts the lexicographical
     * commands to a {@code String} member type.
     * <p>
     * A swapped range with an unbounded boundary converts to a range that is empty by construction.
     * Lettuce renders an unbounded lexicographical boundary by the slot it occupies (lower slot
     * {@code -}, upper slot {@code +}), so a swapped one would flip to the opposite infinity, and —
     * unlike a score boundary — a lexicographical boundary cannot carry its own sentinel: any value is
     * prefixed with {@code [} or {@code (}. Under the Vert.x backend every such query selects an empty
     * range (its effective {@code max} is {@code -}, or its {@code min} is {@code +}), so an empty range
     * preserves the observable behaviour.
     *
     * @param range the Quarkus lexicographical range
     * @param swapped whether to exchange the two boundaries
     * @return the Lettuce range
     */
    public static io.lettuce.core.Range<String> toLettuceLexRange(Range<String> range, boolean swapped) {
        io.lettuce.core.Range.Boundary<String> lower = toLexBoundary(range.getLowerBound(), "-");
        io.lettuce.core.Range.Boundary<String> upper = toLexBoundary(range.getUpperBound(), "+");
        if (!swapped) {
            return io.lettuce.core.Range.from(lower, upper);
        }
        if (lower.isUnbounded() || upper.isUnbounded()) {
            return EMPTY_LEX_RANGE;
        }
        return io.lettuce.core.Range.from(upper, lower);
    }

    /**
     * Parse one formatted score boundary — {@code -inf}, {@code +inf}, {@code (2.5} or {@code 2.5}.
     */
    private static io.lettuce.core.Range.Boundary<Number> toScoreBoundary(String bound, InfiniteScore infinity) {
        if (infinity.toString().equals(bound)) {
            return io.lettuce.core.Range.Boundary.including(infinity);
        }
        if (bound.startsWith("(")) {
            return io.lettuce.core.Range.Boundary.excluding(Double.parseDouble(bound.substring(1)));
        }
        return io.lettuce.core.Range.Boundary.including(Double.parseDouble(bound));
    }

    /**
     * An infinite score that keeps its {@code -inf} / {@code +inf} token in either slot of a
     * {@link io.lettuce.core.Range}. Lettuce special-cases only a {@code null} value and the
     * {@link Double} infinities — all rendered by slot — and emits any other {@link Number} via
     * {@code toString()}, so this value renders the same token wherever it sits.
     */
    private static final class InfiniteScore extends Number {

        static final InfiniteScore NEGATIVE = new InfiniteScore(Double.NEGATIVE_INFINITY, "-inf");
        static final InfiniteScore POSITIVE = new InfiniteScore(Double.POSITIVE_INFINITY, "+inf");

        private final double value;
        private final String token;

        private InfiniteScore(double value, String token) {
            this.value = value;
            this.token = token;
        }

        @Override
        public int intValue() {
            return (int) value;
        }

        @Override
        public long longValue() {
            return (long) value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public double doubleValue() {
            return value;
        }

        @Override
        public String toString() {
            return token;
        }
    }

    /**
     * Parse one formatted lexicographical boundary — {@code -}, {@code +}, {@code (value} or
     * {@code [value}.
     */
    private static io.lettuce.core.Range.Boundary<String> toLexBoundary(String bound, String infinity) {
        if (infinity.equals(bound)) {
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

    private static Aggregation parseAggregation(ZAggregateArgs quarkus) {
        List<Double> weights = new ArrayList<>();
        String aggregate = null;
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i).toString();
            switch (token) {
                case "WEIGHTS" -> {
                    // WEIGHTS is followed by one value per input key; they run until the next keyword.
                    while (i + 1 < tokens.size() && isNumeric(tokens.get(i + 1).toString())) {
                        weights.add(Double.parseDouble(tokens.get(++i).toString()));
                    }
                    if (weights.isEmpty()) {
                        throw new IllegalStateException("Missing value for token: " + token);
                    }
                }
                case "AGGREGATE" -> aggregate = nextToken(tokens, ++i, token);
                default -> throw new IllegalStateException("Unexpected ZAggregateArgs token: " + token);
            }
        }
        return new Aggregation(weights, aggregate);
    }

    private static <T extends io.lettuce.core.ZAggregateArgs> T apply(Aggregation aggregation, T lettuce) {
        if (!aggregation.weights.isEmpty()) {
            double[] weights = new double[aggregation.weights.size()];
            for (int i = 0; i < weights.length; i++) {
                weights[i] = aggregation.weights.get(i);
            }
            lettuce.weights(weights);
        }
        if (aggregation.aggregate != null) {
            switch (aggregation.aggregate) {
                case "SUM" -> lettuce.sum();
                case "MIN" -> lettuce.min();
                case "MAX" -> lettuce.max();
                default -> throw new IllegalStateException("Unexpected aggregate function: " + aggregation.aggregate);
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
            Double.parseDouble(token);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * The {@code WEIGHTS} / {@code AGGREGATE} pair shared by Lettuce's {@code ZAggregateArgs} and
     * {@code ZStoreArgs}.
     */
    private record Aggregation(List<Double> weights, String aggregate) {
    }

    /**
     * The Lettuce-side shape of a Quarkus {@link ZRangeArgs}: whether the reversed command variant must
     * be used, and the {@code LIMIT} to apply.
     */
    public static final class RangeOptions {

        private final boolean reverse;
        private final Limit limit;

        RangeOptions(boolean reverse, Limit limit) {
            this.reverse = reverse;
            this.limit = limit;
        }

        public boolean isReverse() {
            return reverse;
        }

        public Limit limit() {
            return limit;
        }

        public boolean hasLimit() {
            return limit.isLimited();
        }
    }
}
