package io.quarkus.redis.lettuce.runtime.internal.timeseries;

import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.lettuce.core.Range;
import io.lettuce.core.timeseries.TsAggregation;
import io.lettuce.core.timeseries.TsFilter;
import io.lettuce.core.timeseries.TsMAddValue;
import io.lettuce.core.timeseries.TsMGetValue;
import io.lettuce.core.timeseries.TsSample;
import io.lettuce.core.timeseries.TsSeries;
import io.lettuce.core.timeseries.arguments.TsAddArgs;
import io.lettuce.core.timeseries.arguments.TsAlterArgs;
import io.lettuce.core.timeseries.arguments.TsCreateArgs;
import io.lettuce.core.timeseries.arguments.TsIncrByArgs;
import io.lettuce.core.timeseries.arguments.TsMGetArgs;
import io.lettuce.core.timeseries.arguments.TsMRangeArgs;
import io.lettuce.core.timeseries.arguments.TsRangeArgs;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.timeseries.AddArgs;
import io.quarkus.redis.datasource.timeseries.Aggregation;
import io.quarkus.redis.datasource.timeseries.AlterArgs;
import io.quarkus.redis.datasource.timeseries.CreateArgs;
import io.quarkus.redis.datasource.timeseries.Filter;
import io.quarkus.redis.datasource.timeseries.IncrementArgs;
import io.quarkus.redis.datasource.timeseries.MGetArgs;
import io.quarkus.redis.datasource.timeseries.MRangeArgs;
import io.quarkus.redis.datasource.timeseries.RangeArgs;
import io.quarkus.redis.datasource.timeseries.ReactiveTimeSeriesCommands;
import io.quarkus.redis.datasource.timeseries.Sample;
import io.quarkus.redis.datasource.timeseries.SampleGroup;
import io.quarkus.redis.datasource.timeseries.SeriesSample;
import io.quarkus.redis.datasource.timeseries.TimeSeriesRange;
import io.quarkus.redis.lettuce.runtime.internal.AbstractLettuceCommands;
import io.quarkus.redis.lettuce.runtime.internal.LettuceCommand;
import io.quarkus.redis.lettuce.runtime.internal.LettuceConnection;
import io.quarkus.redis.runtime.datasource.Marshaller;
import io.smallrye.mutiny.Uni;

public class LettuceReactiveTimeSeriesCommandsImpl<K> extends AbstractLettuceCommands<K, K>
        implements ReactiveTimeSeriesCommands<K> {

    private final ReactiveRedisDataSource dataSource;

    public LettuceReactiveTimeSeriesCommandsImpl(ReactiveRedisDataSource dataSource,
            LettuceConnection connection, Type keyType) {
        super(connection, keyType, keyType, new Marshaller(keyType));
        this.dataSource = dataSource;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Void> tsCreate(K key, CreateArgs args) {
        return _tsCreate(key, args).toUni();
    }

    LettuceCommand<String, Void> _tsCreate(K key, CreateArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        TsCreateArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceCreateArgs(args);
        return LettuceCommand.discarding(() -> async.tsCreate(marshaller.encode(key), lettuceArgs));
    }

    @Override
    public Uni<Void> tsCreate(K key) {
        return _tsCreate(key).toUni();
    }

    LettuceCommand<String, Void> _tsCreate(K key) {
        return _tsCreate(key, new CreateArgs());
    }

    @Override
    public Uni<Void> tsAdd(K key, long timestamp, double value, AddArgs args) {
        return _tsAdd(key, timestamp, value, args).toUni();
    }

    LettuceCommand<Long, Void> _tsAdd(K key, long timestamp, double value, AddArgs args) {
        nonNull(key, "key");
        positive(timestamp, "timestamp");
        nonNull(args, "args");
        TsAddArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceAddArgs(args);
        return LettuceCommand.discarding(() -> async.tsAdd(marshaller.encode(key), timestamp, value, lettuceArgs));
    }

    @Override
    public Uni<Void> tsAdd(K key, long timestamp, double value) {
        return _tsAdd(key, timestamp, value).toUni();
    }

    LettuceCommand<Long, Void> _tsAdd(K key, long timestamp, double value) {
        return _tsAdd(key, timestamp, value, new AddArgs());
    }

    @Override
    public Uni<Void> tsAdd(K key, double value) {
        return _tsAdd(key, value).toUni();
    }

    LettuceCommand<Long, Void> _tsAdd(K key, double value) {
        return _tsAdd(key, value, new AddArgs());
    }

    @Override
    public Uni<Void> tsAdd(K key, double value, AddArgs args) {
        return _tsAdd(key, value, args).toUni();
    }

    LettuceCommand<Long, Void> _tsAdd(K key, double value, AddArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        TsAddArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceAddArgs(args);
        return LettuceCommand.discarding(() -> async.tsAddNow(marshaller.encode(key), value, lettuceArgs));
    }

    @Override
    public Uni<Void> tsAlter(K key, AlterArgs args) {
        return _tsAlter(key, args).toUni();
    }

    LettuceCommand<String, Void> _tsAlter(K key, AlterArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        TsAlterArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceAlterArgs(args);
        return LettuceCommand.discarding(() -> async.tsAlter(marshaller.encode(key), lettuceArgs));
    }

    @Override
    public Uni<Void> tsCreateRule(K key, K destKey, Aggregation aggregation, Duration bucketDuration) {
        return _tsCreateRule(key, destKey, aggregation, bucketDuration).toUni();
    }

    LettuceCommand<String, Void> _tsCreateRule(K key, K destKey, Aggregation aggregation, Duration bucketDuration) {
        nonNull(key, "key");
        nonNull(destKey, "destKey");
        nonNull(aggregation, "aggregation");
        nonNull(bucketDuration, "bucketDuration");
        TsAggregation lettuceAggregation = LettuceTimeSeriesCommandsConverters.toLettuceAggregation(aggregation);
        return LettuceCommand.discarding(() -> async.tsCreateRule(marshaller.encode(key), marshaller.encode(destKey),
                lettuceAggregation, bucketDuration.toMillis()));
    }

    @Override
    public Uni<Void> tsCreateRule(K key, K destKey, Aggregation aggregation, Duration bucketDuration, long alignTimestamp) {
        return _tsCreateRule(key, destKey, aggregation, bucketDuration, alignTimestamp).toUni();
    }

    LettuceCommand<String, Void> _tsCreateRule(K key, K destKey, Aggregation aggregation, Duration bucketDuration,
            long alignTimestamp) {
        nonNull(key, "key");
        nonNull(destKey, "destKey");
        nonNull(aggregation, "aggregation");
        nonNull(bucketDuration, "bucketDuration");
        positive(alignTimestamp, "alignTimestamp");
        TsAggregation lettuceAggregation = LettuceTimeSeriesCommandsConverters.toLettuceAggregation(aggregation);
        return LettuceCommand.discarding(() -> async.tsCreateRule(marshaller.encode(key), marshaller.encode(destKey),
                lettuceAggregation, bucketDuration.toMillis(), alignTimestamp));
    }

    @Override
    public Uni<Void> tsDecrBy(K key, double value) {
        return _tsDecrBy(key, value).toUni();
    }

    LettuceCommand<Long, Void> _tsDecrBy(K key, double value) {
        return _tsDecrBy(key, value, new IncrementArgs());
    }

    @Override
    public Uni<Void> tsDecrBy(K key, double value, IncrementArgs args) {
        return _tsDecrBy(key, value, args).toUni();
    }

    LettuceCommand<Long, Void> _tsDecrBy(K key, double value, IncrementArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        TsIncrByArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceIncrByArgs(args);
        return LettuceCommand.discarding(() -> async.tsDecrBy(marshaller.encode(key), value, lettuceArgs));
    }

    @Override
    public Uni<Void> tsDel(K key, long fromTimestamp, long toTimestamp) {
        return _tsDel(key, fromTimestamp, toTimestamp).toUni();
    }

    LettuceCommand<Long, Void> _tsDel(K key, long fromTimestamp, long toTimestamp) {
        nonNull(key, "key");
        positive(fromTimestamp, "fromTimestamp");
        positive(toTimestamp, "toTimestamp");
        Range<Long> range = Range.create(fromTimestamp, toTimestamp);
        return LettuceCommand.discarding(() -> async.tsDel(marshaller.encode(key), range));
    }

    @Override
    public Uni<Void> tsDeleteRule(K key, K destKey) {
        return _tsDeleteRule(key, destKey).toUni();
    }

    LettuceCommand<String, Void> _tsDeleteRule(K key, K destKey) {
        nonNull(key, "key");
        nonNull(destKey, "destKey");
        return LettuceCommand.discarding(() -> async.tsDeleteRule(marshaller.encode(key), marshaller.encode(destKey)));
    }

    @Override
    public Uni<Sample> tsGet(K key) {
        return _tsGet(key).toUni();
    }

    LettuceCommand<TsSample, Sample> _tsGet(K key) {
        nonNull(key, "key");
        return LettuceCommand.of(() -> async.tsGet(marshaller.encode(key)), this::decodeTsSample);
    }

    @Override
    public Uni<Sample> tsGet(K key, boolean latest) {
        return _tsGet(key, latest).toUni();
    }

    LettuceCommand<TsSample, Sample> _tsGet(K key, boolean latest) {
        nonNull(key, "key");
        return LettuceCommand.of(() -> async.tsGet(marshaller.encode(key), latest), this::decodeTsSample);
    }

    @Override
    public Uni<Void> tsIncrBy(K key, double value) {
        return _tsIncrBy(key, value).toUni();
    }

    LettuceCommand<Long, Void> _tsIncrBy(K key, double value) {
        return _tsIncrBy(key, value, new IncrementArgs());
    }

    @Override
    public Uni<Void> tsIncrBy(K key, double value, IncrementArgs args) {
        return _tsIncrBy(key, value, args).toUni();
    }

    LettuceCommand<Long, Void> _tsIncrBy(K key, double value, IncrementArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        TsIncrByArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceIncrByArgs(args);
        return LettuceCommand.discarding(() -> async.tsIncrBy(marshaller.encode(key), value, lettuceArgs));
    }

    @SafeVarargs
    @Override
    public final Uni<Void> tsMAdd(SeriesSample<K>... samples) {
        return _tsMAdd(samples).toUni();
    }

    LettuceCommand<List<Long>, Void> _tsMAdd(SeriesSample<K>[] samples) {
        doesNotContainNull(samples, "samples");
        if (samples.length == 0) {
            return LettuceCommand.failing(new IllegalArgumentException("`samples` must not be empty"));
        }
        List<TsMAddValue<byte[]>> lettuceValues = LettuceTimeSeriesCommandsConverters.toLettuceMAddValues(samples, marshaller);
        return LettuceCommand.discarding(() -> async.tsMAdd(lettuceValues));
    }

    @Override
    public Uni<Map<String, SampleGroup>> tsMGet(MGetArgs args, Filter... filters) {
        return _tsMGet(args, filters).toUni();
    }

    LettuceCommand<List<TsMGetValue<byte[]>>, Map<String, SampleGroup>> _tsMGet(MGetArgs args, Filter... filters) {
        nonNull(args, "args");
        doesNotContainNull(filters, "filters");
        if (filters.length == 0) {
            return LettuceCommand.failing(new IllegalArgumentException("`filters` must not be empty"));
        }
        TsMGetArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceMGetArgs(args);
        TsFilter[] lettuceFilters = LettuceTimeSeriesCommandsConverters.toLettuceFilters(filters);
        return LettuceCommand.of(() -> async.tsMGet(lettuceArgs, lettuceFilters), this::decodeTsMGetValues);
    }

    @Override
    public Uni<Map<String, SampleGroup>> tsMGet(Filter... filters) {
        return _tsMGet(filters).toUni();
    }

    LettuceCommand<List<TsMGetValue<byte[]>>, Map<String, SampleGroup>> _tsMGet(Filter... filters) {
        return _tsMGet(new MGetArgs(), filters);
    }

    @Override
    public Uni<Map<String, SampleGroup>> tsMRange(TimeSeriesRange range, Filter... filters) {
        return _tsMRange(range, filters).toUni();
    }

    LettuceCommand<List<TsSeries<byte[]>>, Map<String, SampleGroup>> _tsMRange(TimeSeriesRange range, Filter... filters) {
        nonNull(range, "range");
        doesNotContainNull(filters, "filters");
        if (filters.length == 0) {
            return LettuceCommand.failing(new IllegalArgumentException("`filters` must not be empty"));
        }
        Range<Long> lettuceRange = LettuceTimeSeriesCommandsConverters.toLettuceRange(range);
        TsFilter[] lettuceFilters = LettuceTimeSeriesCommandsConverters.toLettuceFilters(filters);
        return LettuceCommand.of(() -> async.tsMRange(lettuceRange, lettuceFilters), this::decodeTsSeries);
    }

    @Override
    public Uni<Map<String, SampleGroup>> tsMRange(TimeSeriesRange range, MRangeArgs args, Filter... filters) {
        return _tsMRange(range, args, filters).toUni();
    }

    LettuceCommand<List<TsSeries<byte[]>>, Map<String, SampleGroup>> _tsMRange(TimeSeriesRange range, MRangeArgs args,
            Filter... filters) {
        nonNull(range, "range");
        nonNull(args, "args");
        doesNotContainNull(filters, "filters");
        if (filters.length == 0) {
            return LettuceCommand.failing(new IllegalArgumentException("`filters` must not be empty"));
        }
        Range<Long> lettuceRange = LettuceTimeSeriesCommandsConverters.toLettuceRange(range);
        TsMRangeArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceMRangeArgs(args);
        TsFilter[] lettuceFilters = LettuceTimeSeriesCommandsConverters.toLettuceFilters(filters);
        return LettuceCommand.of(() -> async.tsMRange(lettuceRange, lettuceArgs, lettuceFilters), this::decodeTsSeries);
    }

    @Override
    public Uni<Map<String, SampleGroup>> tsMRevRange(TimeSeriesRange range, Filter... filters) {
        return _tsMRevRange(range, filters).toUni();
    }

    LettuceCommand<List<TsSeries<byte[]>>, Map<String, SampleGroup>> _tsMRevRange(TimeSeriesRange range, Filter... filters) {
        nonNull(range, "range");
        doesNotContainNull(filters, "filters");
        if (filters.length == 0) {
            return LettuceCommand.failing(new IllegalArgumentException("`filters` must not be empty"));
        }
        Range<Long> lettuceRange = LettuceTimeSeriesCommandsConverters.toLettuceRange(range);
        TsFilter[] lettuceFilters = LettuceTimeSeriesCommandsConverters.toLettuceFilters(filters);
        return LettuceCommand.of(() -> async.tsMRevRange(lettuceRange, lettuceFilters), this::decodeTsSeries);
    }

    @Override
    public Uni<Map<String, SampleGroup>> tsMRevRange(TimeSeriesRange range, MRangeArgs args, Filter... filters) {
        return _tsMRevRange(range, args, filters).toUni();
    }

    LettuceCommand<List<TsSeries<byte[]>>, Map<String, SampleGroup>> _tsMRevRange(TimeSeriesRange range, MRangeArgs args,
            Filter... filters) {
        nonNull(range, "range");
        nonNull(args, "args");
        doesNotContainNull(filters, "filters");
        if (filters.length == 0) {
            return LettuceCommand.failing(new IllegalArgumentException("`filters` must not be empty"));
        }
        Range<Long> lettuceRange = LettuceTimeSeriesCommandsConverters.toLettuceRange(range);
        TsMRangeArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceMRangeArgs(args);
        TsFilter[] lettuceFilters = LettuceTimeSeriesCommandsConverters.toLettuceFilters(filters);
        return LettuceCommand.of(() -> async.tsMRevRange(lettuceRange, lettuceArgs, lettuceFilters), this::decodeTsSeries);
    }

    @Override
    public Uni<List<K>> tsQueryIndex(Filter... filters) {
        return _tsQueryIndex(filters).toUni();
    }

    LettuceCommand<List<byte[]>, List<K>> _tsQueryIndex(Filter... filters) {
        doesNotContainNull(filters, "filters");
        if (filters.length == 0) {
            return LettuceCommand.failing(new IllegalArgumentException("`filters` must not be empty"));
        }
        TsFilter[] lettuceFilters = LettuceTimeSeriesCommandsConverters.toLettuceFilters(filters);
        return LettuceCommand.of(() -> async.tsQueryIndex(lettuceFilters), this::decodeListOfKeys);
    }

    @Override
    public Uni<List<Sample>> tsRange(K key, TimeSeriesRange range) {
        return _tsRange(key, range).toUni();
    }

    LettuceCommand<List<TsSample>, List<Sample>> _tsRange(K key, TimeSeriesRange range) {
        nonNull(key, "key");
        nonNull(range, "range");
        Range<Long> lettuceRange = LettuceTimeSeriesCommandsConverters.toLettuceRange(range);
        return LettuceCommand.of(() -> async.tsRange(marshaller.encode(key), lettuceRange), this::decodeTsSamples);
    }

    @Override
    public Uni<List<Sample>> tsRange(K key, TimeSeriesRange range, RangeArgs args) {
        return _tsRange(key, range, args).toUni();
    }

    LettuceCommand<List<TsSample>, List<Sample>> _tsRange(K key, TimeSeriesRange range, RangeArgs args) {
        nonNull(key, "key");
        nonNull(range, "range");
        nonNull(args, "args");
        Range<Long> lettuceRange = LettuceTimeSeriesCommandsConverters.toLettuceRange(range);
        TsRangeArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceRangeArgs(args);
        return LettuceCommand.of(() -> async.tsRange(marshaller.encode(key), lettuceRange, lettuceArgs),
                this::decodeTsSamples);
    }

    @Override
    public Uni<List<Sample>> tsRevRange(K key, TimeSeriesRange range) {
        return _tsRevRange(key, range).toUni();
    }

    LettuceCommand<List<TsSample>, List<Sample>> _tsRevRange(K key, TimeSeriesRange range) {
        nonNull(key, "key");
        nonNull(range, "range");
        Range<Long> lettuceRange = LettuceTimeSeriesCommandsConverters.toLettuceRange(range);
        return LettuceCommand.of(() -> async.tsRevRange(marshaller.encode(key), lettuceRange), this::decodeTsSamples);
    }

    @Override
    public Uni<List<Sample>> tsRevRange(K key, TimeSeriesRange range, RangeArgs args) {
        return _tsRevRange(key, range, args).toUni();
    }

    LettuceCommand<List<TsSample>, List<Sample>> _tsRevRange(K key, TimeSeriesRange range, RangeArgs args) {
        nonNull(key, "key");
        nonNull(range, "range");
        nonNull(args, "args");
        Range<Long> lettuceRange = LettuceTimeSeriesCommandsConverters.toLettuceRange(range);
        TsRangeArgs lettuceArgs = LettuceTimeSeriesCommandsConverters.toLettuceRangeArgs(args);
        return LettuceCommand.of(() -> async.tsRevRange(marshaller.encode(key), lettuceRange, lettuceArgs),
                this::decodeTsSamples);
    }

    private List<Sample> decodeTsSamples(List<TsSample> samples) {
        if (samples == null) {
            return null;
        }
        List<Sample> result = new ArrayList<>(samples.size());
        for (TsSample sample : samples) {
            result.add(decodeTsSample(sample));
        }
        return result;
    }

    private Map<String, SampleGroup> decodeTsSeries(List<TsSeries<byte[]>> series) {
        if (series == null) {
            return null;
        }
        if (series.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, SampleGroup> groups = new LinkedHashMap<>();
        for (TsSeries<byte[]> s : series) {
            // a GROUPBY reply is keyed by the "label=value" group, otherwise by the raw series key
            String group = s.isGrouped() ? s.getGroup() : new String(s.getKey(), StandardCharsets.UTF_8);
            Map<String, String> labels = s.getLabels() == null ? new HashMap<>() : new HashMap<>(s.getLabels());
            groups.put(group, new SampleGroup(group, labels, decodeTsSamples(s.getSamples())));
        }
        return groups;
    }

    private Sample decodeTsSample(TsSample sample) {
        return new Sample(sample.getTimestamp(), sample.getValue());
    }

    private Map<String, SampleGroup> decodeTsMGetValues(List<TsMGetValue<byte[]>> values) {
        if (values == null) {
            return null;
        }
        if (values.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, SampleGroup> groups = new LinkedHashMap<>();
        for (TsMGetValue<byte[]> value : values) {
            String group = new String(value.getKey(), StandardCharsets.UTF_8);
            Map<String, String> labels = value.getLabels() == null ? new HashMap<>() : new HashMap<>(value.getLabels());
            List<Sample> samples = new ArrayList<>(1);
            if (value.getSample() != null) {
                samples.add(decodeTsSample(value.getSample()));
            }
            groups.put(group, new SampleGroup(group, labels, samples));
        }
        return groups;
    }

}
