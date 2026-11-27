package io.quarkus.redis.lettuce.runtime.internal.timeseries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.lettuce.core.Range;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.timeseries.TsFilter;
import io.lettuce.core.timeseries.TsMAddValue;
import io.lettuce.core.timeseries.arguments.TsMRangeArgs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.quarkus.redis.datasource.timeseries.AddArgs;
import io.quarkus.redis.datasource.timeseries.Aggregation;
import io.quarkus.redis.datasource.timeseries.AlterArgs;
import io.quarkus.redis.datasource.timeseries.BucketTimestamp;
import io.quarkus.redis.datasource.timeseries.CreateArgs;
import io.quarkus.redis.datasource.timeseries.DuplicatePolicy;
import io.quarkus.redis.datasource.timeseries.Filter;
import io.quarkus.redis.datasource.timeseries.IncrementArgs;
import io.quarkus.redis.datasource.timeseries.MGetArgs;
import io.quarkus.redis.datasource.timeseries.MRangeArgs;
import io.quarkus.redis.datasource.timeseries.RangeArgs;
import io.quarkus.redis.datasource.timeseries.Reducer;
import io.quarkus.redis.datasource.timeseries.SeriesSample;
import io.quarkus.redis.datasource.timeseries.TimeSeriesRange;
import io.quarkus.redis.runtime.datasource.Marshaller;

/**
 * Renders the result of each converter to tokens and compares it with the tokens of the Quarkus argument class, which
 * are what the Vert.x backend sends.
 */
class LettuceTimeSeriesCommandsConvertersTest {

    private static final String FILTER = "team=a";

    // ------------------------------------------------------------------ TS.CREATE

    @Test
    void createArgsWithoutOptionsRenderNothing() {
        assertThat(renderCreate(null)).isEmpty();
        assertThat(renderCreate(new CreateArgs())).isEmpty();
    }

    @Test
    void createArgsRenderTheQuarkusTokens() {
        assertCreate(new CreateArgs().setRetention(Duration.ofSeconds(5)).compressed().chunkSize(1024)
                .duplicatePolicy(DuplicatePolicy.LAST).label("team", "a").label("zone", 3));
        assertCreate(new CreateArgs().forever().uncompressed());
        // a label spelled like an option stays a label
        assertCreate(new CreateArgs().chunkSize(48).label("RETENTION", "LABELS"));
    }

    @ParameterizedTest
    @EnumSource(DuplicatePolicy.class)
    void createArgsRenderEveryDuplicatePolicy(DuplicatePolicy policy) {
        assertCreate(new CreateArgs().duplicatePolicy(policy));
    }

    @Test
    void createArgsRejectAnUnknownToken() {
        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceCreateArgs(new CreateArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("BOGUS");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TS.CREATE").hasMessageContaining("BOGUS");
    }

    // ------------------------------------------------------------------ TS.ADD

    @Test
    void addArgsWithoutOptionsRenderNothing() {
        assertThat(renderAdd(null)).isEmpty();
        assertThat(renderAdd(new AddArgs())).isEmpty();
    }

    @Test
    void addArgsRenderTheQuarkusTokens() {
        assertAdd(new AddArgs().setRetention(Duration.ofSeconds(5)).compressed().chunkSize(1024)
                .onDuplicate(DuplicatePolicy.SUM).label("team", "a").label("zone", 3));
        assertAdd(new AddArgs().setRetention(Duration.ZERO).uncompressed());
        assertAdd(new AddArgs().label("ON_DUPLICATE", "ENCODING"));
    }

    @ParameterizedTest
    @EnumSource(DuplicatePolicy.class)
    void addArgsRenderEveryDuplicatePolicy(DuplicatePolicy policy) {
        assertAdd(new AddArgs().onDuplicate(policy));
    }

    @Test
    void addArgsRejectAnUnknownToken() {
        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceAddArgs(new AddArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("BOGUS");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TS.ADD").hasMessageContaining("BOGUS");
    }

    // ------------------------------------------------------------------ TS.ALTER

    @Test
    void alterArgsWithoutOptionsRenderNothing() {
        assertThat(renderAlter(null)).isEmpty();
        assertThat(renderAlter(new AlterArgs())).isEmpty();
    }

    @Test
    void alterArgsRenderTheQuarkusTokens() {
        assertAlter(new AlterArgs().setRetention(Duration.ofSeconds(5)).chunkSize(1024)
                .duplicatePolicy(DuplicatePolicy.MIN).label("team", "a").label("zone", 3));
        assertAlter(new AlterArgs().forever());
        assertAlter(new AlterArgs().label("CHUNK_SIZE", "DUPLICATE_POLICY"));
    }

    @ParameterizedTest
    @EnumSource(DuplicatePolicy.class)
    void alterArgsRenderEveryDuplicatePolicy(DuplicatePolicy policy) {
        assertAlter(new AlterArgs().duplicatePolicy(policy));
    }

    @Test
    void alterArgsRejectAnUnknownToken() {
        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceAlterArgs(new AlterArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("BOGUS");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TS.ALTER").hasMessageContaining("BOGUS");
    }

    // ------------------------------------------------------------------ TS.INCRBY / TS.DECRBY

    @Test
    void incrByArgsWithoutOptionsRenderNothing() {
        assertThat(renderIncrBy(null)).isEmpty();
        assertThat(renderIncrBy(new IncrementArgs())).isEmpty();
    }

    @Test
    void incrByArgsRenderTheQuarkusTokens() {
        assertIncrBy(new IncrementArgs().setTimestamp(1_700_000_000_000L).setRetention(Duration.ofSeconds(5))
                .uncompressed().chunkSize(1024).label("team", "a").label("zone", 3));
        assertIncrBy(new IncrementArgs().setRetention(Duration.ZERO));
        assertIncrBy(new IncrementArgs().label("TIMESTAMP", "UNCOMPRESSED"));
    }

    @Test
    void incrByArgsRejectAnUnknownToken() {
        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceIncrByArgs(new IncrementArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("BOGUS");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TS.INCRBY").hasMessageContaining("BOGUS");
    }

    // ------------------------------------------------------------------ TS.MADD

    @Test
    void mAddValuesKeepTheTimestampOrUseTheServerTime() {
        @SuppressWarnings("unchecked")
        SeriesSample<String>[] samples = new SeriesSample[] { SeriesSample.from("a", 10L, 1.5), SeriesSample.from("b", 2.5) };

        List<TsMAddValue<byte[]>> values = LettuceTimeSeriesCommandsConverters.toLettuceMAddValues(samples,
                new Marshaller(String.class));

        assertThat(values).hasSize(2);
        assertThat(values.get(0)).isEqualTo(TsMAddValue.of(values.get(0).getKey(), 10L, 1.5));
        assertThat(new String(values.get(0).getKey())).isEqualTo("a");
        // SeriesSample.from(key, value) means "*": the server assigns the timestamp
        assertThat(values.get(1)).isEqualTo(TsMAddValue.now(values.get(1).getKey(), 2.5));
        assertThat(new String(values.get(1).getKey())).isEqualTo("b");
    }

    // ------------------------------------------------------------------ TS.MGET

    @Test
    void mGetArgsWithoutOptionsRenderNothing() {
        assertThat(renderMGet(null)).isEmpty();
        assertThat(renderMGet(new MGetArgs())).isEmpty();
    }

    @Test
    void mGetArgsRenderTheQuarkusTokens() {
        assertMGet(new MGetArgs().latest().withLabels());
        assertMGet(new MGetArgs().latest().selectedLabel("team").selectedLabel("zone"));
    }

    @Test
    void mGetArgsKeepLabelsSpelledLikeOptions() {
        assertThat(renderMGet(new MGetArgs().selectedLabel("LATEST").selectedLabel("WITHLABELS")
                .selectedLabel("SELECTED_LABELS")))
                .containsExactly("SELECTED_LABELS", "LATEST", "WITHLABELS", "SELECTED_LABELS");
    }

    @Test
    void mGetArgsRejectWithLabelsCombinedWithSelectedLabels() {
        assertThatThrownBy(() -> renderMGet(new MGetArgs().withLabels().selectedLabel("team")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mGetArgsRejectAnUnknownToken() {
        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceMGetArgs(new MGetArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("BOGUS");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TS.MGET").hasMessageContaining("BOGUS");
    }

    // ------------------------------------------------------------------ filters, ranges, aggregations

    @Test
    void filtersKeepTheQuarkusExpression() {
        TsFilter[] filters = LettuceTimeSeriesCommandsConverters.toLettuceFilters(new Filter[] {
                Filter.withLabel("team", "a"), Filter.withoutLabel("team", "b"), Filter.withLabel("zone"),
                Filter.withoutLabel("zone"), Filter.withLabelHavingValueFrom("team", "a", "b"),
                Filter.withLabelNotHavingValueFrom("team", "c", "d") });

        assertThat(filters).extracting(TsFilter::toString)
                .containsExactly("team=a", "team!=b", "zone=", "zone!=", "team=(a,b)", "team!=(c,d)");
    }

    @Test
    void rangesMapTheSeriesEdgesToUnboundedBoundaries() {
        Range<Long> full = LettuceTimeSeriesCommandsConverters.toLettuceRange(TimeSeriesRange.fromTimeSeries());
        assertThat(full.getLower().isUnbounded()).isTrue();
        assertThat(full.getUpper().isUnbounded()).isTrue();

        Range<Long> bounded = LettuceTimeSeriesCommandsConverters.toLettuceRange(TimeSeriesRange.fromTimestamps(10, 20));
        assertThat(bounded.getLower().getValue()).isEqualTo(10L);
        assertThat(bounded.getLower().isIncluding()).isTrue();
        assertThat(bounded.getUpper().getValue()).isEqualTo(20L);
        assertThat(bounded.getUpper().isIncluding()).isTrue();

        Range<Long> fromStart = LettuceTimeSeriesCommandsConverters.toLettuceRange(TimeSeriesRange.fromEarliestToTimestamp(20));
        assertThat(fromStart.getLower().isUnbounded()).isTrue();
        assertThat(fromStart.getUpper().getValue()).isEqualTo(20L);

        Range<Long> toEnd = LettuceTimeSeriesCommandsConverters.toLettuceRange(TimeSeriesRange.fromTimestampToLatest(10));
        assertThat(toEnd.getLower().getValue()).isEqualTo(10L);
        assertThat(toEnd.getUpper().isUnbounded()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(Aggregation.class)
    void aggregationsMapByName(Aggregation aggregation) {
        assertThat(LettuceTimeSeriesCommandsConverters.toLettuceAggregation(aggregation).toString())
                .isEqualTo(aggregationToken(aggregation.toString()));
    }

    // ------------------------------------------------------------------ TS.RANGE / TS.REVRANGE

    @Test
    void rangeArgsWithoutOptionsRenderNothing() {
        assertThat(renderRange(null)).isEmpty();
        assertThat(renderRange(new RangeArgs())).isEmpty();
    }

    @Test
    void rangeArgsRenderTheQuarkusTokens() {
        assertRange(new RangeArgs().latest().filterByTimestamp(10, 20, 30).filterByValue(1.5, 2.5).count(7).align(5)
                .aggregation(Aggregation.AVG, Duration.ofMillis(100)).bucketTimestamp(BucketTimestamp.MID).empty());
        assertRange(new RangeArgs().filterByTimestamp(10).count(1));
        assertRange(new RangeArgs().filterByValue(-3, 1e12));
        assertRange(new RangeArgs().alignUsingRangeStart().aggregation(Aggregation.SUM, Duration.ofSeconds(1)));
        assertRange(new RangeArgs().alignUsingRangeEnd().aggregation(Aggregation.SUM, Duration.ofSeconds(1)).empty());
    }

    @Test
    void rangeArgsDropTheOptionsThatNeedAnAggregationLikeTheQuarkusArgs() {
        assertThat(renderRange(new RangeArgs().align(5).bucketTimestamp(BucketTimestamp.LOW).empty().count(3)))
                .containsExactly("COUNT", "3");
    }

    @ParameterizedTest
    @EnumSource(Aggregation.class)
    void rangeArgsRenderEveryAggregation(Aggregation aggregation) {
        assertRange(new RangeArgs().aggregation(aggregation, Duration.ofMillis(100)));
    }

    @ParameterizedTest
    @EnumSource(BucketTimestamp.class)
    void rangeArgsRenderEveryBucketTimestamp(BucketTimestamp bucketTimestamp) {
        assertRange(new RangeArgs().aggregation(Aggregation.MAX, Duration.ofMillis(100)).bucketTimestamp(bucketTimestamp));
    }

    @Test
    void rangeArgsRejectAnUnknownTokenOrBucketTimestamp() {
        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceRangeArgs(new RangeArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("BOGUS");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TS.RANGE").hasMessageContaining("BOGUS");

        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceRangeArgs(new RangeArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("AGGREGATION", "avg", "100", "BUCKETTIMESTAMP", "sideways");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("BUCKETTIMESTAMP")
                .hasMessageContaining("sideways");
    }

    // ------------------------------------------------------------------ TS.MRANGE / TS.MREVRANGE

    @Test
    void mRangeArgsWithoutOptionsRenderOnlyTheFilter() {
        assertThat(renderMRange(null)).containsExactly("FILTER", FILTER);
        assertThat(renderMRange(new MRangeArgs())).containsExactly("FILTER", FILTER);
    }

    @Test
    void mRangeArgsRenderTheQuarkusTokens() {
        assertMRange(new MRangeArgs().latest().filterByTimestamp(10, 20, 30).filterByValue(1.5, 2.5).withLabels().count(7)
                .align(5).aggregation(Aggregation.AVG, Duration.ofMillis(100)).bucketTimestamp(BucketTimestamp.MID).empty()
                .groupBy("team", Reducer.MAX));
        assertMRange(new MRangeArgs().filterByTimestamp(10).selectedLabels("team", "zone"));
        assertMRange(new MRangeArgs().selectedLabels("team").count(7));
        assertMRange(new MRangeArgs().selectedLabels("team", "zone").count(7).alignUsingRangeStart()
                .aggregation(Aggregation.TWA, Duration.ofSeconds(1)).bucketTimestamp(BucketTimestamp.HIGH).empty());
        assertMRange(new MRangeArgs().selectedLabels("team").alignUsingRangeEnd()
                .aggregation(Aggregation.SUM, Duration.ofSeconds(1)));
        assertMRange(new MRangeArgs().selectedLabels("team").aggregation(Aggregation.SUM, Duration.ofSeconds(1)).empty());
    }

    @Test
    void mRangeArgsDropTheOptionsThatNeedAnAggregationLikeTheQuarkusArgs() {
        assertThat(renderMRange(new MRangeArgs().align(5).bucketTimestamp(BucketTimestamp.LOW).empty().count(3)))
                .containsExactly("COUNT", "3", "FILTER", FILTER);
    }

    @Test
    void mRangeArgsKeepLabelsSpelledLikeOptions() {
        assertThat(renderMRange(new MRangeArgs().selectedLabels("COUNT")))
                .containsExactly("SELECTED_LABELS", "COUNT", "FILTER", FILTER);
        assertThat(renderMRange(new MRangeArgs().selectedLabels("EMPTY", "ALIGN", "LATEST", "WITHLABELS", "SELECTED_LABELS",
                "FILTER_BY_TS", "FILTER_BY_VALUE", "AGGREGATION", "BUCKETTIMESTAMP")))
                .containsExactly("SELECTED_LABELS", "EMPTY", "ALIGN", "LATEST", "WITHLABELS", "SELECTED_LABELS",
                        "FILTER_BY_TS", "FILTER_BY_VALUE", "AGGREGATION", "BUCKETTIMESTAMP", "FILTER", FILTER);
    }

    @Test
    void mRangeArgsKeepLabelsSpelledLikeOptionsInFrontOfTheRealOptions() {
        assertThat(renderMRange(new MRangeArgs().selectedLabels("COUNT").count(7)))
                .containsExactly("SELECTED_LABELS", "COUNT", "COUNT", "7", "FILTER", FILTER);
        assertThat(renderMRange(new MRangeArgs().latest().selectedLabels("team", "EMPTY").count(7).align(5)
                .aggregation(Aggregation.MIN, Duration.ofMillis(100)).bucketTimestamp(BucketTimestamp.LOW).empty()))
                .containsExactly("LATEST", "SELECTED_LABELS", "team", "EMPTY", "COUNT", "7", "ALIGN", "5", "AGGREGATION",
                        "MIN", "100", "BUCKETTIMESTAMP", "-", "EMPTY", "FILTER", FILTER);
        assertThat(renderMRange(new MRangeArgs().selectedLabels("ALIGN", "AGGREGATION")
                .aggregation(Aggregation.MIN, Duration.ofMillis(100))))
                .containsExactly("SELECTED_LABELS", "ALIGN", "AGGREGATION", "AGGREGATION", "MIN", "100", "FILTER", FILTER);
    }

    @Test
    void mRangeArgsRenderTheSameTokensWhenTheLabelsSpellOutTheOptions() {
        // "a", "COUNT", "7" cannot be told apart from label "a" followed by COUNT 7, the wire is the same either way
        assertThat(renderMRange(new MRangeArgs().selectedLabels("a", "COUNT", "7")))
                .containsExactly("SELECTED_LABELS", "a", "COUNT", "7", "FILTER", FILTER);
        assertThat(renderMRange(new MRangeArgs().selectedLabels("a", "AGGREGATION", "avg", "100", "EMPTY")))
                .containsExactly("SELECTED_LABELS", "a", "AGGREGATION", "AVG", "100", "EMPTY", "FILTER", FILTER);
    }

    @ParameterizedTest
    @EnumSource(Aggregation.class)
    void mRangeArgsRenderEveryAggregation(Aggregation aggregation) {
        assertMRange(new MRangeArgs().selectedLabels("team").aggregation(aggregation, Duration.ofMillis(100)));
    }

    @ParameterizedTest
    @EnumSource(BucketTimestamp.class)
    void mRangeArgsRenderEveryBucketTimestamp(BucketTimestamp bucketTimestamp) {
        assertMRange(new MRangeArgs().selectedLabels("team").aggregation(Aggregation.MAX, Duration.ofMillis(100))
                .bucketTimestamp(bucketTimestamp));
    }

    @ParameterizedTest
    @EnumSource(Reducer.class)
    void mRangeArgsRenderEveryReducer(Reducer reducer) {
        assertMRange(new MRangeArgs().withLabels().groupBy("team", reducer));
    }

    @Test
    void mRangeArgsRejectWithLabelsCombinedWithSelectedLabels() {
        assertThatThrownBy(() -> renderMRange(new MRangeArgs().withLabels().selectedLabels("team")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mRangeArgsRejectAnUnknownTokenOrBucketTimestamp() {
        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceMRangeArgs(new MRangeArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("BOGUS");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TS.MRANGE").hasMessageContaining("BOGUS");

        assertThatThrownBy(() -> LettuceTimeSeriesCommandsConverters.toLettuceMRangeArgs(new MRangeArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of("AGGREGATION", "avg", "100", "BUCKETTIMESTAMP", "sideways");
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("BUCKETTIMESTAMP")
                .hasMessageContaining("sideways");
    }

    // ------------------------------------------------------------------ helpers

    private static void assertCreate(CreateArgs args) {
        assertThat(renderCreate(args)).containsExactlyElementsOf(expected(args.toArgs()));
    }

    private static void assertAdd(AddArgs args) {
        assertThat(renderAdd(args)).containsExactlyElementsOf(expected(args.toArgs()));
    }

    private static void assertAlter(AlterArgs args) {
        assertThat(renderAlter(args)).containsExactlyElementsOf(expected(args.toArgs()));
    }

    private static void assertIncrBy(IncrementArgs args) {
        assertThat(renderIncrBy(args)).containsExactlyElementsOf(expected(args.toArgs()));
    }

    private static void assertMGet(MGetArgs args) {
        assertThat(renderMGet(args)).containsExactlyElementsOf(expected(args.toArgs()));
    }

    private static void assertRange(RangeArgs args) {
        assertThat(renderRange(args)).containsExactlyElementsOf(expected(args.toArgs()));
    }

    private static void assertMRange(MRangeArgs args) {
        // the same order as the Vert.x backend: options, FILTER clause, GROUPBY clause
        List<Object> tokens = new ArrayList<>(args.toArgs());
        tokens.add("FILTER");
        tokens.add(FILTER);
        tokens.addAll(args.getGroupByClauseArgs());
        assertThat(renderMRange(args)).containsExactlyElementsOf(expected(tokens));
    }

    private static String[] renderCreate(CreateArgs args) {
        return renderToTokens(LettuceTimeSeriesCommandsConverters.toLettuceCreateArgs(args)::build);
    }

    private static String[] renderAdd(AddArgs args) {
        return renderToTokens(LettuceTimeSeriesCommandsConverters.toLettuceAddArgs(args)::build);
    }

    private static String[] renderAlter(AlterArgs args) {
        return renderToTokens(LettuceTimeSeriesCommandsConverters.toLettuceAlterArgs(args)::build);
    }

    private static String[] renderIncrBy(IncrementArgs args) {
        return renderToTokens(LettuceTimeSeriesCommandsConverters.toLettuceIncrByArgs(args)::build);
    }

    private static String[] renderMGet(MGetArgs args) {
        return renderToTokens(LettuceTimeSeriesCommandsConverters.toLettuceMGetArgs(args)::build);
    }

    private static String[] renderRange(RangeArgs args) {
        return renderToTokens(LettuceTimeSeriesCommandsConverters.toLettuceRangeArgs(args)::build);
    }

    private static String[] renderMRange(MRangeArgs args) {
        TsMRangeArgs lettuce = LettuceTimeSeriesCommandsConverters.toLettuceMRangeArgs(args);
        return renderToTokens(commandArgs -> lettuce.build(commandArgs, List.of(TsFilter.raw(FILTER))));
    }

    /**
     * The Quarkus tokens as Lettuce spells them: Redis reads the aggregator, the reducer, the alignment and the bucket
     * timestamp case insensitively and under several names, and Lettuce writes its own spelling of them and of the
     * doubles. Everything else, the labels included, has to be the very same token.
     */
    private static List<String> expected(List<?> quarkusTokens) {
        List<String> expected = new ArrayList<>();
        boolean labels = false;
        for (int i = 0; i < quarkusTokens.size(); i++) {
            String token = String.valueOf(quarkusTokens.get(i));
            expected.add(token);
            if (labels) {
                continue;
            }
            switch (token) {
                case "LABELS" -> labels = true; // trailing label/value pairs, sent as they are
                case "ENCODING" -> expected.add(String.valueOf(quarkusTokens.get(++i)));
                case "ALIGN" -> expected.add(alignToken(String.valueOf(quarkusTokens.get(++i))));
                case "AGGREGATION", "REDUCE" -> expected.add(aggregationToken(String.valueOf(quarkusTokens.get(++i))));
                case "BUCKETTIMESTAMP" -> expected.add(bucketTimestampToken(String.valueOf(quarkusTokens.get(++i))));
                case "UNCOMPRESSED" -> {
                    // TS.INCRBY: the Quarkus args send the legacy flag, Lettuce the ENCODING option
                    expected.set(expected.size() - 1, "ENCODING");
                    expected.add("UNCOMPRESSED");
                }
                case "FILTER_BY_VALUE" -> {
                    expected.add(doubleToken(String.valueOf(quarkusTokens.get(++i))));
                    expected.add(doubleToken(String.valueOf(quarkusTokens.get(++i))));
                }
                default -> {
                }
            }
        }
        return expected;
    }

    private static String aggregationToken(String quarkus) {
        // the Quarkus enums render std_p, Redis and Lettuce spell it STD.P
        return quarkus.toUpperCase(Locale.ROOT).replace('_', '.');
    }

    private static String alignToken(String quarkus) {
        return switch (quarkus) {
            case "-" -> "start";
            case "+" -> "end";
            default -> quarkus;
        };
    }

    private static String bucketTimestampToken(String quarkus) {
        return switch (quarkus) {
            case "low" -> "-";
            case "high" -> "+";
            case "mid" -> "~";
            default -> throw new IllegalArgumentException(quarkus);
        };
    }

    private static String doubleToken(String quarkus) {
        return renderToTokens(args -> args.add(Double.parseDouble(quarkus)))[0];
    }

    /**
     * Encodes the arguments the way Lettuce writes them to the socket and reads the bulk strings back, so the tokens
     * are the ones Redis receives ({@code CommandArgs.toCommandString()} prints keywords Base64 encoded).
     */
    private static String[] renderToTokens(Consumer<CommandArgs<String, String>> builder) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        builder.accept(args);
        ByteBuf buffer = Unpooled.buffer();
        try {
            args.encode(buffer);
            List<String> tokens = new ArrayList<>();
            while (buffer.isReadable()) {
                assertThat((char) buffer.readByte()).isEqualTo('$');
                int length = 0;
                for (byte b = buffer.readByte(); b != '\r'; b = buffer.readByte()) {
                    length = length * 10 + (b - '0');
                }
                buffer.skipBytes(1); // \n
                tokens.add(buffer.readCharSequence(length, StandardCharsets.UTF_8).toString());
                buffer.skipBytes(2); // \r\n
            }
            return tokens.toArray(new String[0]);
        } finally {
            buffer.release();
        }
    }
}
