package io.quarkus.redis.lettuce.runtime.internal.sortedset;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.lettuce.core.Range;
import io.quarkus.redis.datasource.sortedset.ScoreRange;

class LettuceSortedSetCommandsConvertersTest {

    // -------------------------------------------------------------- ScoreRange
    // Lettuce Range is not a renderable command-args object, so these assert through
    // its public boundary accessors instead of wire tokens.

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

    // ------------------------------------------------------ lexicographical Range

    @Test
    void inclusiveLexRange() {
        Range<byte[]> range = LettuceSortedSetCommandsConverters
                .toLettuceLexRange(new io.quarkus.redis.datasource.sortedset.Range<>("b", "d"));
        assertThat(range.getLower().getValue()).isEqualTo("b".getBytes(UTF_8));
        assertThat(range.getLower().isIncluding()).isTrue();
        assertThat(range.getUpper().getValue()).isEqualTo("d".getBytes(UTF_8));
        assertThat(range.getUpper().isIncluding()).isTrue();
    }

    @Test
    void exclusiveLexRange() {
        Range<byte[]> range = LettuceSortedSetCommandsConverters
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
        Range<byte[]> halfOpen = LettuceSortedSetCommandsConverters
                .toLettuceLexRange(new io.quarkus.redis.datasource.sortedset.Range<>("value99", true, null, true));
        assertThat(halfOpen.getLower().getValue()).isEqualTo("value99".getBytes(UTF_8));
        assertThat(halfOpen.getUpper().isUnbounded()).isTrue();
    }

    /** {@code -} is only a sentinel in the lower bound; as an upper bound it stays a plain member. */
    @Test
    void minusAsAnUpperBoundIsAPlainMember() {
        Range<byte[]> range = LettuceSortedSetCommandsConverters
                .toLettuceLexRange(new io.quarkus.redis.datasource.sortedset.Range<>("c", "-"));
        assertThat(range.getLower().getValue()).isEqualTo("c".getBytes(UTF_8));
        assertThat(range.getUpper().getValue()).isEqualTo("-".getBytes(UTF_8));
        assertThat(range.getUpper().isIncluding()).isTrue();
    }
}
