package io.quarkus.redis.runtime.client.lettuce.sortedset;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.lettuce.core.Range;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.sortedset.ScoreRange;
import io.quarkus.redis.datasource.sortedset.ZAddArgs;
import io.quarkus.redis.datasource.sortedset.ZAggregateArgs;

class LettuceSortedSetCommandsConvertersTest {

    // --- ScanArgs ---

    @Test
    void emptyScanArgsSetsNothing() {
        io.lettuce.core.ScanArgs args = LettuceSortedSetCommandsConverters.toLettuceScanArgs(new ScanArgs());
        assertThat(count(args)).isNull();
        assertThat(match(args)).isNull();
    }

    @Test
    void matchAndCount() {
        io.lettuce.core.ScanArgs args = LettuceSortedSetCommandsConverters
                .toLettuceScanArgs(new ScanArgs().count(7).match("keep:*"));
        assertThat(count(args)).isEqualTo(7L);
        assertThat(match(args)).isEqualTo("keep:*".getBytes(UTF_8));
    }

    @Test
    void matchBeforeCountIsOrderIndependent() {
        io.lettuce.core.ScanArgs args = LettuceSortedSetCommandsConverters
                .toLettuceScanArgs(scanArgs("MATCH", "keep:*", "COUNT", "5"));
        assertThat(count(args)).isEqualTo(5L);
        assertThat(match(args)).isEqualTo("keep:*".getBytes(UTF_8));
    }

    @Test
    void danglingScanKeywordWithoutValueIsRejected() {
        ScanArgs dangling = scanArgs("COUNT", "5", "MATCH");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceScanArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: MATCH");
    }

    @Test
    void unknownScanTokenIsRejected() {
        ScanArgs unknown = scanArgs("COUNT", "5", "NOVALUES");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceScanArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected ScanArgs token: NOVALUES");
    }

    // --- SortArgs ---

    @Test
    void emptySortArgsSetsNothing() {
        io.lettuce.core.SortArgs args = LettuceSortedSetCommandsConverters.toLettuceSortArgs(new SortArgs());
        assertThat(by(args)).isNull();
        assertThat(get(args)).isNullOrEmpty();
        // Lettuce initializes the field to Limit.unlimited() rather than leaving it null.
        assertThat(limit(args).isLimited()).isFalse();
        assertThat(alpha(args)).isFalse();
        assertThat(order(args)).isNull();
    }

    @Test
    void allSortOptionsCombined() {
        io.lettuce.core.SortArgs args = LettuceSortedSetCommandsConverters.toLettuceSortArgs(new SortArgs()
                .by("weight_*")
                .limit(1, 3)
                .get("#")
                .descending()
                .alpha());
        assertThat(by(args)).isEqualTo("weight_*");
        assertThat(limit(args).getOffset()).isEqualTo(1);
        assertThat(limit(args).getCount()).isEqualTo(3);
        assertThat(get(args)).containsExactly("#");
        assertThat(order(args)).isEqualTo("DESC");
        assertThat(alpha(args)).isTrue();
    }

    @Test
    void ascendingSortArgs() {
        io.lettuce.core.SortArgs args = LettuceSortedSetCommandsConverters.toLettuceSortArgs(new SortArgs().ascending());
        assertThat(order(args)).isEqualTo("ASC");
    }

    @Test
    void limitWithASingleValueIsTreatedAsACount() {
        io.lettuce.core.SortArgs args = LettuceSortedSetCommandsConverters
                .toLettuceSortArgs(new SortArgs().limit(SortArgs.Limit.of(-1, 4)));
        assertThat(limit(args).getOffset()).isZero();
        assertThat(limit(args).getCount()).isEqualTo(4);
    }

    @Test
    void limitFollowedByAKeywordIsNotMisread() {
        io.lettuce.core.SortArgs args = LettuceSortedSetCommandsConverters
                .toLettuceSortArgs(sortArgs("LIMIT", "9", "ALPHA"));
        assertThat(limit(args).getOffset()).isZero();
        assertThat(limit(args).getCount()).isEqualTo(9);
        assertThat(alpha(args)).isTrue();
    }

    @Test
    void danglingSortKeywordWithoutValueIsRejected() {
        SortArgs dangling = sortArgs("ALPHA", "BY");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceSortArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: BY");
    }

    @Test
    void unknownSortTokenIsRejected() {
        SortArgs unknown = sortArgs("ALPHA", "STORE");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceSortArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected SortArgs token: STORE");
    }

    // --- ZAddArgs ---

    @Test
    void emptyZAddArgsSetsNothing() {
        io.lettuce.core.ZAddArgs args = LettuceSortedSetCommandsConverters.toLettuceZAddArgs(new ZAddArgs());
        assertThat(flag(args, "nx")).isFalse();
        assertThat(flag(args, "xx")).isFalse();
        assertThat(flag(args, "ch")).isFalse();
        assertThat(flag(args, "lt")).isFalse();
        assertThat(flag(args, "gt")).isFalse();
    }

    @Test
    void nxAndChangedZAddArgs() {
        io.lettuce.core.ZAddArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZAddArgs(new ZAddArgs().nx().ch());
        assertThat(flag(args, "nx")).isTrue();
        assertThat(flag(args, "ch")).isTrue();
        assertThat(flag(args, "xx")).isFalse();
    }

    @Test
    void xxAndGreaterThanZAddArgs() {
        io.lettuce.core.ZAddArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZAddArgs(new ZAddArgs().xx().gt());
        assertThat(flag(args, "xx")).isTrue();
        assertThat(flag(args, "gt")).isTrue();
        assertThat(flag(args, "lt")).isFalse();
    }

    @Test
    void lowerThanZAddArgs() {
        io.lettuce.core.ZAddArgs args = LettuceSortedSetCommandsConverters.toLettuceZAddArgs(new ZAddArgs().lt());
        assertThat(flag(args, "lt")).isTrue();
    }

    @Test
    void zAddTokenOrderIsIndependent() {
        io.lettuce.core.ZAddArgs args = LettuceSortedSetCommandsConverters.toLettuceZAddArgs(zAddArgs("CH", "XX"));
        assertThat(flag(args, "ch")).isTrue();
        assertThat(flag(args, "xx")).isTrue();
    }

    @Test
    void unknownZAddTokenIsRejected() {
        ZAddArgs unknown = zAddArgs("NX", "INCR");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceZAddArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected ZAddArgs token: INCR");
    }

    // --- ZAggregateArgs / ZStoreArgs ---

    @Test
    void emptyAggregateArgsSetsNothing() {
        io.lettuce.core.ZAggregateArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZAggregateArgs(new ZAggregateArgs());
        assertThat(weights(args)).isNull();
        assertThat(aggregate(args)).isNull();
    }

    @Test
    void weightsOnly() {
        io.lettuce.core.ZAggregateArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZAggregateArgs(new ZAggregateArgs().weights(2.0, 3.5));
        assertThat(weights(args)).containsExactly(2.0, 3.5);
        assertThat(aggregate(args)).isNull();
    }

    @Test
    void aggregateOnly() {
        io.lettuce.core.ZAggregateArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZAggregateArgs(new ZAggregateArgs().min());
        assertThat(weights(args)).isNull();
        assertThat(aggregate(args)).isEqualTo("MIN");
    }

    @Test
    void weightsAndAggregate() {
        io.lettuce.core.ZAggregateArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZAggregateArgs(new ZAggregateArgs().weights(1, 2, 3).max());
        assertThat(weights(args)).containsExactly(1.0, 2.0, 3.0);
        assertThat(aggregate(args)).isEqualTo("MAX");
    }

    @Test
    void aggregateBeforeWeightsIsOrderIndependent() {
        io.lettuce.core.ZAggregateArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZAggregateArgs(aggregateArgs("AGGREGATE", "SUM", "WEIGHTS", "2.0", "3.0"));
        assertThat(weights(args)).containsExactly(2.0, 3.0);
        assertThat(aggregate(args)).isEqualTo("SUM");
    }

    @Test
    void theSameOptionsConvertToZStoreArgs() {
        io.lettuce.core.ZStoreArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZStoreArgs(new ZAggregateArgs().weights(2.0, 3.0).sum());
        assertThat(weights(args)).containsExactly(2.0, 3.0);
        assertThat(aggregate(args)).isEqualTo("SUM");
    }

    @Test
    void danglingWeightsWithoutValueIsRejected() {
        ZAggregateArgs dangling = aggregateArgs("WEIGHTS");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: WEIGHTS");
    }

    @Test
    void danglingAggregateWithoutValueIsRejected() {
        ZAggregateArgs dangling = aggregateArgs("WEIGHTS", "1.0", "AGGREGATE");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: AGGREGATE");
    }

    @Test
    void unknownAggregateFunctionIsRejected() {
        ZAggregateArgs unknown = aggregateArgs("AGGREGATE", "AVG");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected aggregate function: AVG");
    }

    @Test
    void unknownAggregateTokenIsRejected() {
        ZAggregateArgs unknown = aggregateArgs("WITHSCORES");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected ZAggregateArgs token: WITHSCORES");
    }

    // --- ScoreRange ---

    @Test
    void inclusiveScoreRange() {
        Range<Number> range = LettuceSortedSetCommandsConverters.toLettuceScoreRange(ScoreRange.from(1.0, 3.0));
        assertThat(range.getLower().getValue()).isEqualTo(1.0);
        assertThat(range.getLower().isIncluding()).isTrue();
        assertThat(range.getUpper().getValue()).isEqualTo(3.0);
        assertThat(range.getUpper().isIncluding()).isTrue();
    }

    @Test
    void exclusiveScoreRange() {
        Range<Number> range = LettuceSortedSetCommandsConverters
                .toLettuceScoreRange(new ScoreRange<>(1.0, false, 3.0, false));
        assertThat(range.getLower().getValue()).isEqualTo(1.0);
        assertThat(range.getLower().isIncluding()).isFalse();
        assertThat(range.getUpper().getValue()).isEqualTo(3.0);
        assertThat(range.getUpper().isIncluding()).isFalse();
    }

    /**
     * An unbounded score range converts to inclusive infinite boundaries — Lettuce writes those as
     * the {@code -inf} / {@code +inf} tokens, matching what the Vert.x backend sends.
     */
    @Test
    void unboundedScoreRangeBecomesInfiniteBoundaries() {
        for (ScoreRange<Double> unbounded : List.of(ScoreRange.unbounded(),
                new ScoreRange<Double>(null, null),
                new ScoreRange<>(NEGATIVE_INFINITY, POSITIVE_INFINITY))) {
            Range<Number> range = LettuceSortedSetCommandsConverters.toLettuceScoreRange(unbounded);
            assertThat(range.getLower().getValue()).isEqualTo(NEGATIVE_INFINITY);
            assertThat(range.getLower().isIncluding()).isTrue();
            assertThat(range.getUpper().getValue()).isEqualTo(POSITIVE_INFINITY);
            assertThat(range.getUpper().isIncluding()).isTrue();
        }
    }

    /** An infinite boundary is always inclusive, even in an otherwise exclusive range. */
    @Test
    void halfInfiniteScoreRange() {
        Range<Number> range = LettuceSortedSetCommandsConverters
                .toLettuceScoreRange(new ScoreRange<>(NEGATIVE_INFINITY, false, 3.0, false));
        assertThat(range.getLower().getValue()).isEqualTo(NEGATIVE_INFINITY);
        assertThat(range.getLower().isIncluding()).isTrue();
        assertThat(range.getUpper().getValue()).isEqualTo(3.0);
        assertThat(range.getUpper().isIncluding()).isFalse();
    }

    // --- lexicographical Range ---

    @Test
    void inclusiveLexRange() {
        Range<String> range = LettuceSortedSetCommandsConverters
                .toLettuceLexRange(new io.quarkus.redis.datasource.sortedset.Range<>("b", "d"));
        assertThat(range.getLower().getValue()).isEqualTo("b");
        assertThat(range.getLower().isIncluding()).isTrue();
        assertThat(range.getUpper().getValue()).isEqualTo("d");
        assertThat(range.getUpper().isIncluding()).isTrue();
    }

    @Test
    void exclusiveLexRange() {
        Range<String> range = LettuceSortedSetCommandsConverters
                .toLettuceLexRange(new io.quarkus.redis.datasource.sortedset.Range<>("b", false, "d", false));
        assertThat(range.getLower().isIncluding()).isFalse();
        assertThat(range.getUpper().isIncluding()).isFalse();
    }

    @Test
    void unboundedLexRange() {
        assertThat(LettuceSortedSetCommandsConverters
                .toLettuceLexRange(io.quarkus.redis.datasource.sortedset.Range.unbounded()).isUnbounded())
                .isTrue();
        // The `-` / `+` sentinels and a `null` bound are the two other spellings of "unbounded".
        assertThat(LettuceSortedSetCommandsConverters
                .toLettuceLexRange(new io.quarkus.redis.datasource.sortedset.Range<>("-", "+")).isUnbounded())
                .isTrue();
        Range<String> halfOpen = LettuceSortedSetCommandsConverters
                .toLettuceLexRange(new io.quarkus.redis.datasource.sortedset.Range<>("value99", true, null, true));
        assertThat(halfOpen.getLower().getValue()).isEqualTo("value99");
        assertThat(halfOpen.getUpper().isUnbounded()).isTrue();
    }

    /** {@code -} is only a sentinel in the lower bound; as an upper bound it stays a plain member. */
    @Test
    void minusAsAnUpperBoundIsAPlainMember() {
        Range<String> range = LettuceSortedSetCommandsConverters
                .toLettuceLexRange(new io.quarkus.redis.datasource.sortedset.Range<>("c", "-"));
        assertThat(range.getLower().getValue()).isEqualTo("c");
        assertThat(range.getUpper().getValue()).isEqualTo("-");
        assertThat(range.getUpper().isIncluding()).isTrue();
    }

    // --- Helpers ---

    private static Long count(io.lettuce.core.ScanArgs args) {
        return (Long) read(io.lettuce.core.ScanArgs.class, args, "count");
    }

    private static byte[] match(io.lettuce.core.ScanArgs args) {
        return (byte[]) read(io.lettuce.core.ScanArgs.class, args, "match");
    }

    private static String by(io.lettuce.core.SortArgs args) {
        return (String) read(io.lettuce.core.SortArgs.class, args, "by");
    }

    @SuppressWarnings("unchecked")
    private static List<String> get(io.lettuce.core.SortArgs args) {
        return (List<String>) read(io.lettuce.core.SortArgs.class, args, "get");
    }

    private static io.lettuce.core.Limit limit(io.lettuce.core.SortArgs args) {
        return (io.lettuce.core.Limit) read(io.lettuce.core.SortArgs.class, args, "limit");
    }

    private static boolean alpha(io.lettuce.core.SortArgs args) {
        return (Boolean) read(io.lettuce.core.SortArgs.class, args, "alpha");
    }

    private static String order(io.lettuce.core.SortArgs args) {
        Object keyword = read(io.lettuce.core.SortArgs.class, args, "order");
        return keyword == null ? null : keyword.toString();
    }

    private static boolean flag(io.lettuce.core.ZAddArgs args, String name) {
        return (Boolean) read(io.lettuce.core.ZAddArgs.class, args, name);
    }

    @SuppressWarnings("unchecked")
    private static List<Double> weights(io.lettuce.core.ZAggregateArgs args) {
        return (List<Double>) read(io.lettuce.core.ZAggregateArgs.class, args, "weights");
    }

    private static String aggregate(io.lettuce.core.ZAggregateArgs args) {
        Object value = read(io.lettuce.core.ZAggregateArgs.class, args, "aggregate");
        return value == null ? null : value.toString();
    }

    private static Object read(Class<?> type, Object args, String name) {
        try {
            Field field = type.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(args);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Cannot read Lettuce " + type.getSimpleName() + "." + name, e);
        }
    }

    /** A {@link ScanArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static ScanArgs scanArgs(String... tokens) {
        return new ScanArgs() {
            @Override
            public List<String> toArgs() {
                return List.of(tokens);
            }
        };
    }

    /** A {@link SortArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static SortArgs sortArgs(String... tokens) {
        return new SortArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of(tokens);
            }
        };
    }

    /** A {@link ZAddArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static ZAddArgs zAddArgs(String... tokens) {
        return new ZAddArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of(tokens);
            }
        };
    }

    /** A {@link ZAggregateArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static ZAggregateArgs aggregateArgs(String... tokens) {
        return new ZAggregateArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of(tokens);
            }
        };
    }
}
