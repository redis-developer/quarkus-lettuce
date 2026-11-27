package io.quarkus.redis.lettuce.runtime.internal.timeseries;

import java.time.Duration;

import io.quarkus.redis.datasource.timeseries.AddArgs;
import io.quarkus.redis.datasource.timeseries.Aggregation;
import io.quarkus.redis.datasource.timeseries.AlterArgs;
import io.quarkus.redis.datasource.timeseries.CreateArgs;
import io.quarkus.redis.datasource.timeseries.Filter;
import io.quarkus.redis.datasource.timeseries.IncrementArgs;
import io.quarkus.redis.datasource.timeseries.MGetArgs;
import io.quarkus.redis.datasource.timeseries.MRangeArgs;
import io.quarkus.redis.datasource.timeseries.RangeArgs;
import io.quarkus.redis.datasource.timeseries.ReactiveTransactionalTimeSeriesCommands;
import io.quarkus.redis.datasource.timeseries.SeriesSample;
import io.quarkus.redis.datasource.timeseries.TimeSeriesRange;
import io.quarkus.redis.datasource.transactions.ReactiveTransactionalRedisDataSource;
import io.quarkus.redis.lettuce.runtime.internal.datasource.LettuceTransactionHolder;
import io.smallrye.mutiny.Uni;

public class LettuceReactiveTransactionalTimeSeriesCommandsImpl<K> implements ReactiveTransactionalTimeSeriesCommands<K> {

    private final ReactiveTransactionalRedisDataSource dataSource;
    private final LettuceReactiveTimeSeriesCommandsImpl<K> reactive;
    private final LettuceTransactionHolder tx;

    public LettuceReactiveTransactionalTimeSeriesCommandsImpl(ReactiveTransactionalRedisDataSource dataSource,
            LettuceReactiveTimeSeriesCommandsImpl<K> reactive, LettuceTransactionHolder tx) {
        this.dataSource = dataSource;
        this.reactive = reactive;
        this.tx = tx;
    }

    @Override
    public ReactiveTransactionalRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Void> tsCreate(K key, CreateArgs args) {
        return tx.enqueue(reactive._tsCreate(key, args));
    }

    @Override
    public Uni<Void> tsCreate(K key) {
        return tx.enqueue(reactive._tsCreate(key));
    }

    @Override
    public Uni<Void> tsAdd(K key, long timestamp, double value, AddArgs args) {
        return tx.enqueue(reactive._tsAdd(key, timestamp, value, args));
    }

    @Override
    public Uni<Void> tsAdd(K key, long timestamp, double value) {
        return tx.enqueue(reactive._tsAdd(key, timestamp, value));
    }

    @Override
    public Uni<Void> tsAdd(K key, double value) {
        return tx.enqueue(reactive._tsAdd(key, value));
    }

    @Override
    public Uni<Void> tsAlter(K key, AlterArgs args) {
        return tx.enqueue(reactive._tsAlter(key, args));
    }

    @Override
    public Uni<Void> tsCreateRule(K key, K destKey, Aggregation aggregation, Duration bucketDuration) {
        return tx.enqueue(reactive._tsCreateRule(key, destKey, aggregation, bucketDuration));
    }

    @Override
    public Uni<Void> tsCreateRule(K key, K destKey, Aggregation aggregation, Duration bucketDuration, long alignTimestamp) {
        return tx.enqueue(reactive._tsCreateRule(key, destKey, aggregation, bucketDuration, alignTimestamp));
    }

    @Override
    public Uni<Void> tsDecrBy(K key, double value) {
        return tx.enqueue(reactive._tsDecrBy(key, value));
    }

    @Override
    public Uni<Void> tsDecrBy(K key, double value, IncrementArgs args) {
        return tx.enqueue(reactive._tsDecrBy(key, value, args));
    }

    @Override
    public Uni<Void> tsDel(K key, long fromTimestamp, long toTimestamp) {
        return tx.enqueue(reactive._tsDel(key, fromTimestamp, toTimestamp));
    }

    @Override
    public Uni<Void> tsDeleteRule(K key, K destKey) {
        return tx.enqueue(reactive._tsDeleteRule(key, destKey));
    }

    @Override
    public Uni<Void> tsGet(K key) {
        return tx.enqueue(reactive._tsGet(key));
    }

    @Override
    public Uni<Void> tsGet(K key, boolean latest) {
        return tx.enqueue(reactive._tsGet(key, latest));
    }

    @Override
    public Uni<Void> tsIncrBy(K key, double value) {
        return tx.enqueue(reactive._tsIncrBy(key, value));
    }

    @Override
    public Uni<Void> tsIncrBy(K key, double value, IncrementArgs args) {
        return tx.enqueue(reactive._tsIncrBy(key, value, args));
    }

    @SafeVarargs
    @Override
    public final Uni<Void> tsMAdd(SeriesSample<K>... samples) {
        return tx.enqueue(reactive._tsMAdd(samples));
    }

    @Override
    public Uni<Void> tsMGet(MGetArgs args, Filter... filters) {
        return tx.enqueue(reactive._tsMGet(args, filters));
    }

    @Override
    public Uni<Void> tsMGet(Filter... filters) {
        return tx.enqueue(reactive._tsMGet(filters));
    }

    @Override
    public Uni<Void> tsMRange(TimeSeriesRange range, Filter... filters) {
        return tx.enqueue(reactive._tsMRange(range, filters));
    }

    @Override
    public Uni<Void> tsMRange(TimeSeriesRange range, MRangeArgs args, Filter... filters) {
        return tx.enqueue(reactive._tsMRange(range, args, filters));
    }

    @Override
    public Uni<Void> tsMRevRange(TimeSeriesRange range, Filter... filters) {
        return tx.enqueue(reactive._tsMRevRange(range, filters));
    }

    @Override
    public Uni<Void> tsMRevRange(TimeSeriesRange range, MRangeArgs args, Filter... filters) {
        return tx.enqueue(reactive._tsMRevRange(range, args, filters));
    }

    @Override
    public Uni<Void> tsQueryIndex(Filter... filters) {
        return tx.enqueue(reactive._tsQueryIndex(filters));
    }

    @Override
    public Uni<Void> tsRange(K key, TimeSeriesRange range) {
        return tx.enqueue(reactive._tsRange(key, range));
    }

    @Override
    public Uni<Void> tsRange(K key, TimeSeriesRange range, RangeArgs args) {
        return tx.enqueue(reactive._tsRange(key, range, args));
    }

    @Override
    public Uni<Void> tsRevRange(K key, TimeSeriesRange range) {
        return tx.enqueue(reactive._tsRevRange(key, range));
    }

    @Override
    public Uni<Void> tsRevRange(K key, TimeSeriesRange range, RangeArgs args) {
        return tx.enqueue(reactive._tsRevRange(key, range, args));
    }

}
