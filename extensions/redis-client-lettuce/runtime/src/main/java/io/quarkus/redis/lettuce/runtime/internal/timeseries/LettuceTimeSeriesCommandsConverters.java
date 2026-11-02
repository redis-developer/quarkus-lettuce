package io.quarkus.redis.lettuce.runtime.internal.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.LongConsumer;

import io.lettuce.core.Range;
import io.lettuce.core.timeseries.TsAggregation;
import io.lettuce.core.timeseries.TsAggregationType;
import io.lettuce.core.timeseries.TsBucketTimestamp;
import io.lettuce.core.timeseries.TsDuplicatePolicyType;
import io.lettuce.core.timeseries.TsEncodingType;
import io.lettuce.core.timeseries.TsFilter;
import io.lettuce.core.timeseries.TsMAddValue;
import io.lettuce.core.timeseries.TsReducerType;
import io.lettuce.core.timeseries.arguments.TsAddArgs;
import io.lettuce.core.timeseries.arguments.TsAlterArgs;
import io.lettuce.core.timeseries.arguments.TsCreateArgs;
import io.lettuce.core.timeseries.arguments.TsIncrByArgs;
import io.lettuce.core.timeseries.arguments.TsMGetArgs;
import io.lettuce.core.timeseries.arguments.TsMRangeArgs;
import io.lettuce.core.timeseries.arguments.TsRangeArgs;
import io.quarkus.redis.datasource.timeseries.AddArgs;
import io.quarkus.redis.datasource.timeseries.Aggregation;
import io.quarkus.redis.datasource.timeseries.AlterArgs;
import io.quarkus.redis.datasource.timeseries.CreateArgs;
import io.quarkus.redis.datasource.timeseries.Filter;
import io.quarkus.redis.datasource.timeseries.IncrementArgs;
import io.quarkus.redis.datasource.timeseries.MGetArgs;
import io.quarkus.redis.datasource.timeseries.MRangeArgs;
import io.quarkus.redis.datasource.timeseries.RangeArgs;
import io.quarkus.redis.datasource.timeseries.SeriesSample;
import io.quarkus.redis.datasource.timeseries.TimeSeriesRange;
import io.quarkus.redis.runtime.datasource.Marshaller;

final class LettuceTimeSeriesCommandsConverters {

    private LettuceTimeSeriesCommandsConverters() {
        // Utility class
    }

    static TsCreateArgs toLettuceCreateArgs(CreateArgs quarkus) {
        TsCreateArgs lettuce = new TsCreateArgs();
        if (quarkus == null) {
            return lettuce;
        }
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            switch (String.valueOf(tokens.get(i))) {
                case "RETENTION":
                    lettuce.retention(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "ENCODING":
                    lettuce.encoding(TsEncodingType.valueOf(String.valueOf(tokens.get(++i))));
                    break;
                case "CHUNK_SIZE":
                    lettuce.chunkSize(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "DUPLICATE_POLICY":
                    lettuce.duplicatePolicy(TsDuplicatePolicyType.valueOf(String.valueOf(tokens.get(++i))));
                    break;
                case "LABELS":
                    // labels are always the trailing option, emitted as label/value pairs
                    for (i++; i + 1 < tokens.size(); i += 2) {
                        lettuce.label(String.valueOf(tokens.get(i)), String.valueOf(tokens.get(i + 1)));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported TS.CREATE argument: " + tokens.get(i));
            }
        }
        return lettuce;
    }

    static TsAddArgs toLettuceAddArgs(AddArgs quarkus) {
        TsAddArgs lettuce = new TsAddArgs();
        if (quarkus == null) {
            return lettuce;
        }
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            switch (String.valueOf(tokens.get(i))) {
                case "RETENTION":
                    lettuce.retention(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "ENCODING":
                    lettuce.encoding(TsEncodingType.valueOf(String.valueOf(tokens.get(++i))));
                    break;
                case "CHUNK_SIZE":
                    lettuce.chunkSize(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "ON_DUPLICATE":
                    lettuce.onDuplicate(TsDuplicatePolicyType.valueOf(String.valueOf(tokens.get(++i))));
                    break;
                case "LABELS":
                    // labels are always the trailing option, emitted as label/value pairs
                    for (i++; i + 1 < tokens.size(); i += 2) {
                        lettuce.label(String.valueOf(tokens.get(i)), String.valueOf(tokens.get(i + 1)));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported TS.ADD argument: " + tokens.get(i));
            }
        }
        return lettuce;
    }

    static TsAlterArgs toLettuceAlterArgs(AlterArgs quarkus) {
        TsAlterArgs lettuce = new TsAlterArgs();
        if (quarkus == null) {
            return lettuce;
        }
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            switch (String.valueOf(tokens.get(i))) {
                case "RETENTION":
                    lettuce.retention(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "CHUNK_SIZE":
                    lettuce.chunkSize(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "DUPLICATE_POLICY":
                    lettuce.duplicatePolicy(TsDuplicatePolicyType.valueOf(String.valueOf(tokens.get(++i))));
                    break;
                case "LABELS":
                    // labels are always the trailing option, emitted as label/value pairs
                    for (i++; i + 1 < tokens.size(); i += 2) {
                        lettuce.label(String.valueOf(tokens.get(i)), String.valueOf(tokens.get(i + 1)));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported TS.ALTER argument: " + tokens.get(i));
            }
        }
        return lettuce;
    }

    static TsAggregation toLettuceAggregation(Aggregation quarkus) {
        return TsAggregationType.valueOf(quarkus.name());
    }

    static TsIncrByArgs toLettuceIncrByArgs(IncrementArgs quarkus) {
        TsIncrByArgs lettuce = new TsIncrByArgs();
        if (quarkus == null) {
            return lettuce;
        }
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            switch (String.valueOf(tokens.get(i))) {
                case "TIMESTAMP":
                    lettuce.timestamp(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "RETENTION":
                    lettuce.retention(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "UNCOMPRESSED":
                    lettuce.encoding(TsEncodingType.UNCOMPRESSED);
                    break;
                case "CHUNK_SIZE":
                    lettuce.chunkSize(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "LABELS":
                    // labels are always the trailing option, emitted as label/value pairs
                    for (i++; i + 1 < tokens.size(); i += 2) {
                        lettuce.label(String.valueOf(tokens.get(i)), String.valueOf(tokens.get(i + 1)));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported TS.INCRBY/TS.DECRBY argument: " + tokens.get(i));
            }
        }
        return lettuce;
    }

    static <K> List<TsMAddValue<byte[]>> toLettuceMAddValues(SeriesSample<K>[] samples, Marshaller marshaller) {
        List<TsMAddValue<byte[]>> values = new ArrayList<>(samples.length);
        for (SeriesSample<K> sample : samples) {
            byte[] key = marshaller.encode(sample.key());
            if (sample.timestamp() == Long.MAX_VALUE) {
                // SeriesSample.from(key, value) uses Long.MAX_VALUE as the "server time" marker (TS.MADD key * value)
                values.add(TsMAddValue.now(key, sample.value()));
            } else {
                values.add(TsMAddValue.of(key, sample.timestamp(), sample.value()));
            }
        }
        return values;
    }

    static TsMGetArgs toLettuceMGetArgs(MGetArgs quarkus) {
        TsMGetArgs lettuce = new TsMGetArgs();
        if (quarkus == null) {
            return lettuce;
        }
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            switch (String.valueOf(tokens.get(i))) {
                case "LATEST":
                    lettuce.latest();
                    break;
                case "WITHLABELS":
                    lettuce.withLabels();
                    break;
                case "SELECTED_LABELS":
                    // the selected labels are always the trailing option, one token per label
                    String[] selected = new String[tokens.size() - i - 1];
                    for (int j = 0; j < selected.length; j++) {
                        selected[j] = String.valueOf(tokens.get(++i));
                    }
                    lettuce.selectedLabels(selected);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported TS.MGET argument: " + tokens.get(i));
            }
        }
        return lettuce;
    }

    static TsFilter[] toLettuceFilters(Filter[] quarkus) {
        TsFilter[] lettuce = new TsFilter[quarkus.length];
        for (int i = 0; i < quarkus.length; i++) {
            lettuce[i] = TsFilter.raw(quarkus[i].toString());
        }
        return lettuce;
    }

    static Range<Long> toLettuceRange(TimeSeriesRange quarkus) {
        // TimeSeriesRange#toArgs() renders the two bounds as "-"/"+" for the series edges or as a timestamp
        List<String> tokens = quarkus.toArgs();
        return Range.from(toLettuceBoundary(tokens.get(0)), toLettuceBoundary(tokens.get(1)));
    }

    private static Range.Boundary<Long> toLettuceBoundary(String token) {
        if ("-".equals(token) || "+".equals(token)) {
            return Range.Boundary.unbounded();
        }
        return Range.Boundary.including(Long.parseLong(token));
    }

    static TsRangeArgs toLettuceRangeArgs(RangeArgs quarkus) {
        TsRangeArgs lettuce = new TsRangeArgs();
        if (quarkus == null) {
            return lettuce;
        }
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            switch (String.valueOf(tokens.get(i))) {
                case "LATEST":
                    lettuce.latest();
                    break;
                case "FILTER_BY_TS":
                    List<Long> timestamps = new ArrayList<>();
                    while (i + 1 < tokens.size() && !RANGE_KEYWORDS.contains(String.valueOf(tokens.get(i + 1)))) {
                        timestamps.add(Long.parseLong(String.valueOf(tokens.get(++i))));
                    }
                    lettuce.filterByTimestamps(timestamps.stream().mapToLong(Long::longValue).toArray());
                    break;
                case "FILTER_BY_VALUE":
                    double min = Double.parseDouble(String.valueOf(tokens.get(++i)));
                    double max = Double.parseDouble(String.valueOf(tokens.get(++i)));
                    lettuce.filterByValue(min, max);
                    break;
                case "COUNT":
                    lettuce.count(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "ALIGN":
                    applyAlign(String.valueOf(tokens.get(++i)), lettuce::alignStart, lettuce::alignEnd, lettuce::align);
                    break;
                case "AGGREGATION":
                    TsAggregation aggregation = toLettuceAggregation(String.valueOf(tokens.get(++i)));
                    long bucketDuration = Long.parseLong(String.valueOf(tokens.get(++i)));
                    lettuce.aggregation(bucketDuration, aggregation);
                    break;
                case "BUCKETTIMESTAMP":
                    lettuce.bucketTimestamp(toLettuceBucketTimestamp(String.valueOf(tokens.get(++i))));
                    break;
                case "EMPTY":
                    lettuce.empty();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported TS.RANGE/TS.REVRANGE argument: " + tokens.get(i));
            }
        }
        return lettuce;
    }

    static TsMRangeArgs toLettuceMRangeArgs(MRangeArgs quarkus) {
        TsMRangeArgs lettuce = new TsMRangeArgs();
        if (quarkus == null) {
            return lettuce;
        }
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            switch (String.valueOf(tokens.get(i))) {
                case "LATEST":
                    lettuce.latest();
                    break;
                case "FILTER_BY_TS":
                    List<Long> timestamps = new ArrayList<>();
                    while (i + 1 < tokens.size() && !MRANGE_KEYWORDS.contains(String.valueOf(tokens.get(i + 1)))) {
                        timestamps.add(Long.parseLong(String.valueOf(tokens.get(++i))));
                    }
                    lettuce.filterByTimestamps(timestamps.stream().mapToLong(Long::longValue).toArray());
                    break;
                case "FILTER_BY_VALUE":
                    double min = Double.parseDouble(String.valueOf(tokens.get(++i)));
                    double max = Double.parseDouble(String.valueOf(tokens.get(++i)));
                    lettuce.filterByValue(min, max);
                    break;
                case "WITHLABELS":
                    lettuce.withLabels();
                    break;
                case "SELECTED_LABELS":
                    List<String> selected = new ArrayList<>();
                    while (i + 1 < tokens.size() && !MRANGE_KEYWORDS.contains(String.valueOf(tokens.get(i + 1)))) {
                        selected.add(String.valueOf(tokens.get(++i)));
                    }
                    lettuce.selectedLabels(selected.toArray(new String[0]));
                    break;
                case "COUNT":
                    lettuce.count(Long.parseLong(String.valueOf(tokens.get(++i))));
                    break;
                case "ALIGN":
                    applyAlign(String.valueOf(tokens.get(++i)), lettuce::alignStart, lettuce::alignEnd, lettuce::align);
                    break;
                case "AGGREGATION":
                    TsAggregation aggregation = toLettuceAggregation(String.valueOf(tokens.get(++i)));
                    long bucketDuration = Long.parseLong(String.valueOf(tokens.get(++i)));
                    lettuce.aggregation(bucketDuration, aggregation);
                    break;
                case "BUCKETTIMESTAMP":
                    lettuce.bucketTimestamp(toLettuceBucketTimestamp(String.valueOf(tokens.get(++i))));
                    break;
                case "EMPTY":
                    lettuce.empty();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported TS.MRANGE/TS.MREVRANGE argument: " + tokens.get(i));
            }
        }
        // the GROUPBY clause is emitted separately because it must follow the FILTER clause on the wire
        List<String> groupBy = quarkus.getGroupByClauseArgs();
        if (!groupBy.isEmpty()) {
            lettuce.groupBy(groupBy.get(1), TsReducerType.valueOf(groupBy.get(3).toUpperCase(Locale.ROOT)));
        }
        return lettuce;
    }

    private static final Set<String> RANGE_KEYWORDS = Set.of("LATEST", "FILTER_BY_TS", "FILTER_BY_VALUE", "COUNT",
            "ALIGN", "AGGREGATION", "BUCKETTIMESTAMP", "EMPTY");

    private static final Set<String> MRANGE_KEYWORDS = Set.of("LATEST", "FILTER_BY_TS", "FILTER_BY_VALUE", "WITHLABELS",
            "SELECTED_LABELS", "COUNT", "ALIGN", "AGGREGATION", "BUCKETTIMESTAMP", "EMPTY");

    private static void applyAlign(String token, Runnable alignStart, Runnable alignEnd, LongConsumer align) {
        switch (token) {
            case "-":
                alignStart.run();
                break;
            case "+":
                alignEnd.run();
                break;
            default:
                align.accept(Long.parseLong(token));
        }
    }

    private static TsAggregation toLettuceAggregation(String token) {
        return TsAggregationType.valueOf(token.toUpperCase(Locale.ROOT));
    }

    private static TsBucketTimestamp toLettuceBucketTimestamp(String token) {
        return switch (token.toLowerCase(Locale.ROOT)) {
            case "low", "-" -> TsBucketTimestamp.START;
            case "high", "+" -> TsBucketTimestamp.END;
            case "mid", "~" -> TsBucketTimestamp.MID;
            default -> throw new IllegalArgumentException("Unsupported BUCKETTIMESTAMP value: " + token);
        };
    }

}
