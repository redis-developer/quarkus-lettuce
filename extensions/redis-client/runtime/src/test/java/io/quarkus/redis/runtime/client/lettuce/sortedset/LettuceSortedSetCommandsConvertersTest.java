package io.quarkus.redis.runtime.client.lettuce.sortedset;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.lettuce.core.Range;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.sortedset.ScoreRange;
import io.quarkus.redis.datasource.sortedset.ZAddArgs;
import io.quarkus.redis.datasource.sortedset.ZAggregateArgs;

/**
 * Unit tests for {@link LettuceSortedSetCommandsConverters}.
 */
class LettuceSortedSetCommandsConvertersTest {

    // ---------------------------------------------------------------- ScanArgs

    @Test
    void emptyScanArgsSetsNothing() {
        assertThat(renderScanArgs(new ScanArgs())).isEmpty();
    }

    @Test
    void matchAndCount() {
        assertThat(renderScanArgs(new ScanArgs().count(7).match("keep:*")))
                .containsExactly("MATCH", base64("keep:*"), "COUNT", "7");
    }

    @Test
    void matchBeforeCountIsOrderIndependent() {
        assertThat(renderScanArgs(scanArgs("MATCH", "keep:*", "COUNT", "5")))
                .containsExactly("MATCH", base64("keep:*"), "COUNT", "5");
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

    // ---------------------------------------------------------------- SortArgs

    @Test
    void emptySortArgsSetsNothing() {
        assertThat(renderSortArgs(new SortArgs())).isEmpty();
    }

    @Test
    void allSortOptionsCombined() {
        assertThat(renderSortArgs(new SortArgs()
                .by("weight_*")
                .limit(1, 3)
                .get("#")
                .descending()
                .alpha()))
                .containsExactly("BY", "weight_*", "GET", "#", "LIMIT", "1", "3", "DESC", "ALPHA");
    }

    @Test
    void ascendingSortArgs() {
        assertThat(renderSortArgs(new SortArgs().ascending())).containsExactly("ASC");
    }

    /** {@code SortArgs.Limit} emits a single value — the count — when its offset is -1. */
    @Test
    void limitWithASingleValueIsTreatedAsACount() {
        assertThat(renderSortArgs(new SortArgs().limit(SortArgs.Limit.of(-1, 4))))
                .containsExactly("LIMIT", "0", "4");
    }

    @Test
    void limitFollowedByAKeywordIsNotMisread() {
        assertThat(renderSortArgs(sortArgs("LIMIT", "9", "ALPHA")))
                .containsExactly("LIMIT", "0", "9", "ALPHA");
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

    // ---------------------------------------------------------------- ZAddArgs

    @Test
    void emptyZAddArgsSetsNothing() {
        assertThat(renderZAddArgs(new ZAddArgs())).isEmpty();
    }

    @Test
    void nxAndChangedZAddArgs() {
        assertThat(renderZAddArgs(new ZAddArgs().nx().ch())).containsExactly("NX", "CH");
    }

    @Test
    void xxAndGreaterThanZAddArgs() {
        assertThat(renderZAddArgs(new ZAddArgs().xx().gt())).containsExactly("XX", "GT");
    }

    @Test
    void lowerThanZAddArgs() {
        assertThat(renderZAddArgs(new ZAddArgs().lt())).containsExactly("LT");
    }

    @Test
    void zAddTokenOrderIsIndependent() {
        assertThat(renderZAddArgs(zAddArgs("CH", "XX"))).containsExactly("XX", "CH");
    }

    @Test
    void unknownZAddTokenIsRejected() {
        ZAddArgs unknown = zAddArgs("NX", "INCR");
        assertThatThrownBy(() -> LettuceSortedSetCommandsConverters.toLettuceZAddArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected ZAddArgs token: INCR");
    }

    // ------------------------------------------------- ZAggregateArgs / ZStoreArgs

    @Test
    void emptyAggregateArgsSetsNothing() {
        assertThat(renderZAggregateArgs(new ZAggregateArgs())).isEmpty();
    }

    /** {@code toCommandString()} renders weights via {@code Double.toString}, so 2.0 stays "2.0". */
    @Test
    void weightsOnly() {
        assertThat(renderZAggregateArgs(new ZAggregateArgs().weights(2.0, 3.5)))
                .containsExactly("WEIGHTS", "2.0", "3.5");
    }

    @Test
    void aggregateOnly() {
        assertThat(renderZAggregateArgs(new ZAggregateArgs().min())).containsExactly("AGGREGATE", "MIN");
    }

    @Test
    void weightsAndAggregate() {
        assertThat(renderZAggregateArgs(new ZAggregateArgs().weights(1, 2, 3).max()))
                .containsExactly("WEIGHTS", "1.0", "2.0", "3.0", "AGGREGATE", "MAX");
    }

    @Test
    void aggregateBeforeWeightsIsOrderIndependent() {
        assertThat(renderZAggregateArgs(aggregateArgs("AGGREGATE", "SUM", "WEIGHTS", "2.0", "3.0")))
                .containsExactly("WEIGHTS", "2.0", "3.0", "AGGREGATE", "SUM");
    }

    @Test
    void theSameOptionsConvertToZStoreArgs() {
        io.lettuce.core.ZStoreArgs args = LettuceSortedSetCommandsConverters
                .toLettuceZStoreArgs(new ZAggregateArgs().weights(2.0, 3.0).sum());
        assertThat(renderToTokens(args::build)).containsExactly("WEIGHTS", "2.0", "3.0", "AGGREGATE", "SUM");
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

    // ------------------------------------------------------------------ helpers

    private static String[] renderScanArgs(ScanArgs quarkus) {
        return renderToTokens(LettuceSortedSetCommandsConverters.toLettuceScanArgs(quarkus)::build);
    }

    private static String[] renderSortArgs(SortArgs quarkus) {
        return renderToTokens(LettuceSortedSetCommandsConverters.toLettuceSortArgs(quarkus)::build);
    }

    private static String[] renderZAddArgs(ZAddArgs quarkus) {
        return renderToTokens(LettuceSortedSetCommandsConverters.toLettuceZAddArgs(quarkus)::build);
    }

    private static String[] renderZAggregateArgs(ZAggregateArgs quarkus) {
        return renderToTokens(LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(quarkus)::build);
    }

    private static String[] renderToTokens(java.util.function.Consumer<CommandArgs<String, String>> builder) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        builder.accept(args);
        // CommandArgs.toCommandString() renders tokens space-separated, unquoted
        String rendered = args.toCommandString();
        if (rendered == null || rendered.isEmpty()) {
            return new String[0];
        }
        return rendered.split(" ");
    }

    /** Lettuce stores the MATCH pattern as bytes, which {@code toCommandString()} renders Base64-encoded. */
    private static String base64(String pattern) {
        return Base64.getEncoder().encodeToString(pattern.getBytes(UTF_8));
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
