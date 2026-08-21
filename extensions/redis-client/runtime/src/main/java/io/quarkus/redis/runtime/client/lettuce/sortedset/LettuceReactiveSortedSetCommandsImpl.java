package io.quarkus.redis.runtime.client.lettuce.sortedset;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.quarkus.redis.runtime.datasource.Validation.validateTimeout;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ZPopArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.KeyValue;
import io.quarkus.redis.datasource.sortedset.Range;
import io.quarkus.redis.datasource.sortedset.ReactiveSortedSetCommands;
import io.quarkus.redis.datasource.sortedset.ReactiveZScanCursor;
import io.quarkus.redis.datasource.sortedset.ScoreRange;
import io.quarkus.redis.datasource.sortedset.ScoredValue;
import io.quarkus.redis.datasource.sortedset.ZAddArgs;
import io.quarkus.redis.datasource.sortedset.ZAggregateArgs;
import io.quarkus.redis.datasource.sortedset.ZRangeArgs;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveSortedSetCommands}, on top of
 * {@link RedisSortedSetAsyncCommands} plus {@link RedisKeyAsyncCommands} for {@code SORT}.
 *
 * @param <K> the key type
 * @param <V> the type of the scored member
 */
public class LettuceReactiveSortedSetCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveSortedSetCommands<K, V> {

    private static final ZAggregateArgs DEFAULT_INSTANCE_AGG = new ZAggregateArgs();
    private static final ZRangeArgs DEFAULT_INSTANCE_RANGE = new ZRangeArgs();

    private final ReactiveRedisDataSource dataSource;

    private final Type valueType;

    private final RedisSortedSetAsyncCommands<K, V> sortedSet = async;

    /**
     * {@code SORT} lives in Lettuce's key commands, not its sorted set commands.
     */
    private final RedisKeyAsyncCommands<K, V> sortable = async;

    public LettuceReactiveSortedSetCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection, Type valueType) {
        super(connection);
        this.dataSource = dataSource;
        this.valueType = valueType;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Boolean> zadd(K key, double score, V value) {
        return zadd(key, new ZAddArgs(), score, value);
    }

    @Override
    public Uni<Integer> zadd(K key, Map<V, Double> items) {
        return zadd(key, new ZAddArgs(), items);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> zadd(K key, ScoredValue<V>... items) {
        return zadd(key, new ZAddArgs(), items);
    }

    @Override
    public Uni<Boolean> zadd(K key, ZAddArgs args, double score, V value) {
        return LettuceResult.toUni(_zadd(key, args, score, value)).map(LettuceReactiveSortedSetCommandsImpl::asBoolean);
    }

    Supplier<RedisFuture<Long>> _zadd(K key, ZAddArgs args, double score, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(args, "args");
        io.lettuce.core.ZAddArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZAddArgs(args);
        double normalized = normalizeScore(score);
        return () -> sortedSet.zadd(key, lettuceArgs, normalized, value);
    }

    Supplier<RedisFuture<Long>> _zadd(K key, double score, V value) {
        return _zadd(key, new ZAddArgs(), score, value);
    }

    @Override
    public Uni<Integer> zadd(K key, ZAddArgs args, Map<V, Double> items) {
        return LettuceResult.toUni(_zadd(key, args, items)).map(Long::intValue);
    }

    Supplier<RedisFuture<Long>> _zadd(K key, ZAddArgs args, Map<V, Double> items) {
        nonNull(key, "key");
        nonNull(items, "items");
        nonNull(args, "args");

        io.lettuce.core.ZAddArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZAddArgs(args);
        List<io.lettuce.core.ScoredValue<V>> entries = new ArrayList<>(items.size());
        for (Map.Entry<V, Double> entry : items.entrySet()) {
            nonNull(entry.getValue(), "value from items");
            entries.add(io.lettuce.core.ScoredValue.just(normalizeScore(entry.getValue()), entry.getKey()));
        }

        @SuppressWarnings("unchecked")
        io.lettuce.core.ScoredValue<V>[] array = entries.toArray(new io.lettuce.core.ScoredValue[0]);
        return () -> sortedSet.zadd(key, lettuceArgs, array);
    }

    Supplier<RedisFuture<Long>> _zadd(K key, Map<V, Double> items) {
        return _zadd(key, new ZAddArgs(), items);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> zadd(K key, ZAddArgs args, ScoredValue<V>... items) {
        return LettuceResult.toUni(_zadd(key, args, items)).map(Long::intValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zadd(K key, ZAddArgs args, ScoredValue<V>... items) {
        nonNull(key, "key");
        nonNull(items, "items");
        nonNull(args, "args");

        io.lettuce.core.ZAddArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZAddArgs(args);
        List<io.lettuce.core.ScoredValue<V>> entries = new ArrayList<>(items.length);
        for (ScoredValue<V> item : items) {
            nonNull(item.value, "value from scored value");
            entries.add(io.lettuce.core.ScoredValue.just(normalizeScore(item.score), item.value));
        }

        @SuppressWarnings("unchecked")
        io.lettuce.core.ScoredValue<V>[] array = entries.toArray(new io.lettuce.core.ScoredValue[0]);
        return () -> sortedSet.zadd(key, lettuceArgs, array);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zadd(K key, ScoredValue<V>... items) {
        return _zadd(key, new ZAddArgs(), items);
    }

    @Override
    public Uni<Double> zaddincr(K key, double score, V value) {
        return zaddincr(key, new ZAddArgs(), score, value);
    }

    @Override
    public Uni<Double> zaddincr(K key, ZAddArgs args, double score, V value) {
        return LettuceResult.toUni(_zaddincr(key, args, score, value));
    }

    Supplier<RedisFuture<Double>> _zaddincr(K key, ZAddArgs args, double score, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(args, "args");
        io.lettuce.core.ZAddArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZAddArgs(args);
        double normalized = normalizeScore(score);
        return () -> sortedSet.zaddincr(key, lettuceArgs, normalized, value);
    }

    Supplier<RedisFuture<Double>> _zaddincr(K key, double score, V value) {
        return _zaddincr(key, new ZAddArgs(), score, value);
    }

    @Override
    public Uni<Long> zcard(K key) {
        return LettuceResult.toUni(_zcard(key)).map(LettuceReactiveSortedSetCommandsImpl::orZero);
    }

    Supplier<RedisFuture<Long>> _zcard(K key) {
        nonNull(key, "key");
        return () -> sortedSet.zcard(key);
    }

    @Override
    public Uni<Long> zcount(K key, ScoreRange<Double> range) {
        return LettuceResult.toUni(_zcount(key, range));
    }

    Supplier<RedisFuture<Long>> _zcount(K key, ScoreRange<Double> range) {
        nonNull(key, "key");
        nonNull(range, "range");
        io.lettuce.core.Range<Number> lettuceRange = LettuceSortedSetCommandsConverters.toLettuceScoreRange(range);
        return () -> sortedSet.zcount(key, lettuceRange);
    }

    @SafeVarargs
    @Override
    public final Uni<List<V>> zdiff(K... keys) {
        return LettuceResult.toUni(_zdiff(keys)).map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<V>>> _zdiff(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> sortedSet.zdiff(keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zdiffWithScores(K... keys) {
        return LettuceResult.toUni(_zdiffWithScores(keys)).map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zdiffWithScores(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> sortedSet.zdiffWithScores(keys);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> zdiffstore(K destination, K... keys) {
        return LettuceResult.toUni(_zdiffstore(destination, keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zdiffstore(K destination, K... keys) {
        nonNull(destination, "destination");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> sortedSet.zdiffstore(destination, keys);
    }

    @Override
    public Uni<Double> zincrby(K key, double increment, V value) {
        return LettuceResult.toUni(_zincrby(key, increment, value));
    }

    Supplier<RedisFuture<Double>> _zincrby(K key, double increment, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        double normalized = normalizeScore(increment);
        return () -> sortedSet.zincrby(key, normalized, value);
    }

    @SafeVarargs
    @Override
    public final Uni<List<V>> zinter(ZAggregateArgs args, K... keys) {
        return LettuceResult.toUni(_zinter(args, keys)).map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<V>>> _zinter(ZAggregateArgs args, K... keys) {
        nonNull(args, "args");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        io.lettuce.core.ZAggregateArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(args);
        return () -> sortedSet.zinter(lettuceArgs, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<V>> zinter(K... keys) {
        return zinter(DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<V>>> _zinter(K... keys) {
        return _zinter(DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zinterWithScores(ZAggregateArgs arguments, K... keys) {
        return LettuceResult.toUni(_zinterWithScores(arguments, keys))
                .map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zinterWithScores(ZAggregateArgs arguments,
            K... keys) {
        nonNull(arguments, "arguments");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        io.lettuce.core.ZAggregateArgs lettuceArgs = LettuceSortedSetCommandsConverters
                .toLettuceZAggregateArgs(arguments);
        return () -> sortedSet.zinterWithScores(lettuceArgs, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zinterWithScores(K... keys) {
        return zinterWithScores(DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zinterWithScores(K... keys) {
        return _zinterWithScores(DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> zintercard(K... keys) {
        return LettuceResult.toUni(_zintercard(keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zintercard(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> sortedSet.zintercard(keys);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> zintercard(long limit, K... keys) {
        return LettuceResult.toUni(_zintercard(limit, keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zintercard(long limit, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        positive(limit, "limit");
        return () -> sortedSet.zintercard(limit, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> zinterstore(K destination, ZAggregateArgs arguments, K... keys) {
        return LettuceResult.toUni(_zinterstore(destination, arguments, keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zinterstore(K destination, ZAggregateArgs arguments, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        nonNull(arguments, "arguments");
        nonNull(destination, "destination");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        io.lettuce.core.ZStoreArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZStoreArgs(arguments);
        return () -> sortedSet.zinterstore(destination, lettuceArgs, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> zinterstore(K destination, K... keys) {
        return zinterstore(destination, DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zinterstore(K destination, K... keys) {
        return _zinterstore(destination, DEFAULT_INSTANCE_AGG, keys);
    }

    @Override
    public Uni<Long> zlexcount(K key, Range<String> range) {
        return LettuceResult.toUni(_zlexcount(key, range));
    }

    Supplier<RedisFuture<Long>> _zlexcount(K key, Range<String> range) {
        nonNull(key, "key");
        nonNull(range, "range");
        requireStringMembersFor("zlexcount");
        @SuppressWarnings("unchecked")
        io.lettuce.core.Range<V> lettuceRange = (io.lettuce.core.Range<V>) LettuceSortedSetCommandsConverters
                .toLettuceLexRange(range);
        return () -> sortedSet.zlexcount(key, lettuceRange);
    }

    @SafeVarargs
    @Override
    public final Uni<ScoredValue<V>> zmpopMin(K... keys) {
        return LettuceResult.toUni(_zmpopMin(keys)).map(LettuceReactiveSortedSetCommandsImpl::popped);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>>>> _zmpopMin(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        return () -> sortedSet.zmpop(ZPopArgs.Builder.min(), keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zmpopMin(int count, K... keys) {
        return LettuceResult.toUni(_zmpopMin(count, keys)).map(LettuceReactiveSortedSetCommandsImpl::poppedList);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, List<io.lettuce.core.ScoredValue<V>>>>> _zmpopMin(int count,
            K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        positive(count, "count");
        return () -> sortedSet.zmpop(count, ZPopArgs.Builder.min(), keys);
    }

    @SafeVarargs
    @Override
    public final Uni<ScoredValue<V>> zmpopMax(K... keys) {
        return LettuceResult.toUni(_zmpopMax(keys)).map(LettuceReactiveSortedSetCommandsImpl::popped);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>>>> _zmpopMax(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        return () -> sortedSet.zmpop(ZPopArgs.Builder.max(), keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zmpopMax(int count, K... keys) {
        return LettuceResult.toUni(_zmpopMax(count, keys)).map(LettuceReactiveSortedSetCommandsImpl::poppedList);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, List<io.lettuce.core.ScoredValue<V>>>>> _zmpopMax(int count,
            K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        positive(count, "count");
        return () -> sortedSet.zmpop(count, ZPopArgs.Builder.max(), keys);
    }

    @SafeVarargs
    @Override
    public final Uni<ScoredValue<V>> bzmpopMin(Duration timeout, K... keys) {
        return LettuceResult.toUni(_bzmpopMin(timeout, keys)).map(LettuceReactiveSortedSetCommandsImpl::popped);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>>>> _bzmpopMin(Duration timeout,
            K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        long seconds = timeout.toSeconds();
        return () -> sortedSet.bzmpop(seconds, ZPopArgs.Builder.min(), keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> bzmpopMin(Duration timeout, int count, K... keys) {
        return LettuceResult.toUni(_bzmpopMin(timeout, count, keys))
                .map(LettuceReactiveSortedSetCommandsImpl::poppedList);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, List<io.lettuce.core.ScoredValue<V>>>>> _bzmpopMin(
            Duration timeout, int count, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        long seconds = timeout.toSeconds();
        return () -> sortedSet.bzmpop(seconds, (long) count, ZPopArgs.Builder.min(), keys);
    }

    @SafeVarargs
    @Override
    public final Uni<ScoredValue<V>> bzmpopMax(Duration timeout, K... keys) {
        return LettuceResult.toUni(_bzmpopMax(timeout, keys)).map(LettuceReactiveSortedSetCommandsImpl::popped);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>>>> _bzmpopMax(Duration timeout,
            K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        long seconds = timeout.toSeconds();
        return () -> sortedSet.bzmpop(seconds, ZPopArgs.Builder.max(), keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> bzmpopMax(Duration timeout, int count, K... keys) {
        return LettuceResult.toUni(_bzmpopMax(timeout, count, keys))
                .map(LettuceReactiveSortedSetCommandsImpl::poppedList);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, List<io.lettuce.core.ScoredValue<V>>>>> _bzmpopMax(
            Duration timeout, int count, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        long seconds = timeout.toSeconds();
        return () -> sortedSet.bzmpop(seconds, (long) count, ZPopArgs.Builder.max(), keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<Double>> zmscore(K key, V... values) {
        return LettuceResult.toUni(_zmscore(key, values)).map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<Double>>> _zmscore(K key, V... values) {
        nonNull(key, "key");
        notNullOrEmpty(values, "values");
        return () -> sortedSet.zmscore(key, values);
    }

    @Override
    public Uni<ScoredValue<V>> zpopmax(K key) {
        return LettuceResult.toUni(_zpopmax(key)).map(LettuceReactiveSortedSetCommandsImpl::poppedOrEmpty);
    }

    Supplier<RedisFuture<io.lettuce.core.ScoredValue<V>>> _zpopmax(K key) {
        nonNull(key, "key");
        return () -> sortedSet.zpopmax(key);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zpopmax(K key, int count) {
        return LettuceResult.toUni(_zpopmax(key, count)).map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zpopmax(K key, int count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> sortedSet.zpopmax(key, count);
    }

    @Override
    public Uni<ScoredValue<V>> zpopmin(K key) {
        return LettuceResult.toUni(_zpopmin(key)).map(LettuceReactiveSortedSetCommandsImpl::poppedOrEmpty);
    }

    Supplier<RedisFuture<io.lettuce.core.ScoredValue<V>>> _zpopmin(K key) {
        nonNull(key, "key");
        return () -> sortedSet.zpopmin(key);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zpopmin(K key, int count) {
        return LettuceResult.toUni(_zpopmin(key, count)).map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zpopmin(K key, int count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> sortedSet.zpopmin(key, count);
    }

    @Override
    public Uni<V> zrandmember(K key) {
        return LettuceResult.toUni(_zrandmember(key));
    }

    Supplier<RedisFuture<V>> _zrandmember(K key) {
        nonNull(key, "key");
        return () -> sortedSet.zrandmember(key);
    }

    @Override
    public Uni<List<V>> zrandmember(K key, int count) {
        return LettuceResult.toUni(_zrandmember(key, count)).map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _zrandmember(K key, int count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> sortedSet.zrandmember(key, count);
    }

    @Override
    public Uni<ScoredValue<V>> zrandmemberWithScores(K key) {
        return LettuceResult.toUni(_zrandmemberWithScores(key)).map(LettuceReactiveSortedSetCommandsImpl::poppedOrEmpty);
    }

    Supplier<RedisFuture<io.lettuce.core.ScoredValue<V>>> _zrandmemberWithScores(K key) {
        nonNull(key, "key");
        return () -> sortedSet.zrandmemberWithScores(key);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zrandmemberWithScores(K key, int count) {
        return LettuceResult.toUni(_zrandmemberWithScores(key, count))
                .map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zrandmemberWithScores(K key, int count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> sortedSet.zrandmemberWithScores(key, count);
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, ScoredValue<V>>> bzpopmin(Duration timeout, K... keys) {
        return LettuceResult.toUni(_bzpopmin(timeout, keys)).map(LettuceReactiveSortedSetCommandsImpl::toKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>>>> _bzpopmin(Duration timeout,
            K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        long seconds = timeout.toSeconds();
        return () -> sortedSet.bzpopmin(seconds, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, ScoredValue<V>>> bzpopmax(Duration timeout, K... keys) {
        return LettuceResult.toUni(_bzpopmax(timeout, keys)).map(LettuceReactiveSortedSetCommandsImpl::toKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>>>> _bzpopmax(Duration timeout,
            K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        long seconds = timeout.toSeconds();
        return () -> sortedSet.bzpopmax(seconds, keys);
    }

    @Override
    public Uni<List<V>> zrange(K key, long start, long stop, ZRangeArgs args) {
        return LettuceResult.toUni(_zrange(key, start, stop, args)).map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _zrange(K key, long start, long stop, ZRangeArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        //TODO requires lettuce#3681
        throw new UnsupportedOperationException("Operation not supported");
    }

    Supplier<RedisFuture<List<V>>> _zrange(K key, long start, long stop) {
        return _zrange(key, start, stop, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zrangeWithScores(K key, long start, long stop, ZRangeArgs args) {
        return LettuceResult.toUni(_zrangeWithScores(key, start, stop, args))
                .map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zrangeWithScores(K key, long start, long stop,
            ZRangeArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        //TODO requires lettuce#3681
        throw new UnsupportedOperationException("Operation not supported");
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zrangeWithScores(K key, long start, long stop) {
        return _zrangeWithScores(key, start, stop, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<V>> zrange(K key, long start, long stop) {
        return zrange(key, start, stop, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zrangeWithScores(K key, long start, long stop) {
        return zrangeWithScores(key, start, stop, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<V>> zrangebylex(K key, Range<String> range, ZRangeArgs args) {
        return LettuceResult.toUni(_zrangebylex(key, range, args))
                .map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _zrangebylex(K key, Range<String> range, ZRangeArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        nonNull(range, "range");
        requireStringMembersFor("zrangebylex");
        //TODO requires lettuce#3681
        throw new UnsupportedOperationException("Operation not supported");
    }

    Supplier<RedisFuture<List<V>>> _zrangebylex(K key, Range<String> range) {
        return _zrangebylex(key, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<V>> zrangebylex(K key, Range<String> range) {
        return zrangebylex(key, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<V>> zrangebyscore(K key, ScoreRange<Double> range, ZRangeArgs args) {
        return LettuceResult.toUni(_zrangebyscore(key, range, args))
                .map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _zrangebyscore(K key, ScoreRange<Double> range, ZRangeArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        nonNull(range, "range");
        //TODO requires lettuce#3681
        throw new UnsupportedOperationException("Operation not supported");
    }

    Supplier<RedisFuture<List<V>>> _zrangebyscore(K key, ScoreRange<Double> range) {
        return _zrangebyscore(key, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, ScoreRange<Double> range, ZRangeArgs args) {
        return LettuceResult.toUni(_zrangebyscoreWithScores(key, range, args))
                .map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zrangebyscoreWithScores(K key, ScoreRange<Double> range,
            ZRangeArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        nonNull(range, "range");
        //TODO requires lettuce#3681
        throw new UnsupportedOperationException("Operation not supported");
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zrangebyscoreWithScores(K key,
            ScoreRange<Double> range) {
        return _zrangebyscoreWithScores(key, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<V>> zrangebyscore(K key, ScoreRange<Double> range) {
        return zrangebyscore(key, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, ScoreRange<Double> range) {
        return zrangebyscoreWithScores(key, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<Long> zrangestore(K dst, K src, long min, long max, ZRangeArgs args) {
        return LettuceResult.toUni(_zrangestore(dst, src, min, max, args));
    }

    Supplier<RedisFuture<Long>> _zrangestore(K dst, K src, long min, long max, ZRangeArgs args) {
        nonNull(dst, "dst");
        nonNull(src, "src");
        nonNull(args, "args");
        //TODO requires lettuce#3681
        throw new UnsupportedOperationException("Operation not supported");
    }

    Supplier<RedisFuture<Long>> _zrangestore(K dst, K src, long min, long max) {
        return _zrangestore(dst, src, min, max, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<Long> zrangestore(K dst, K src, long min, long max) {
        return zrangestore(dst, src, min, max, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<Long> zrangestorebylex(K dst, K src, Range<String> range, ZRangeArgs args) {
        return LettuceResult.toUni(_zrangestorebylex(dst, src, range, args));
    }

    Supplier<RedisFuture<Long>> _zrangestorebylex(K dst, K src, Range<String> range, ZRangeArgs args) {
        nonNull(dst, "dst");
        nonNull(src, "src");
        nonNull(range, "range");
        nonNull(args, "args");
        requireStringMembersFor("zrangestorebylex");
        //TODO requires lettuce#3681
        throw new UnsupportedOperationException("Operation not supported");
    }

    Supplier<RedisFuture<Long>> _zrangestorebylex(K dst, K src, Range<String> range) {
        return _zrangestorebylex(dst, src, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<Long> zrangestorebylex(K dst, K src, Range<String> range) {
        return zrangestorebylex(dst, src, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<Long> zrangestorebyscore(K dst, K src, ScoreRange<Double> range, ZRangeArgs args) {
        return LettuceResult.toUni(_zrangestorebyscore(dst, src, range, args));
    }

    Supplier<RedisFuture<Long>> _zrangestorebyscore(K dst, K src, ScoreRange<Double> range, ZRangeArgs args) {
        nonNull(dst, "dst");
        nonNull(src, "src");
        nonNull(range, "range");
        nonNull(args, "args");
        //TODO requires lettuce#3681
        throw new UnsupportedOperationException("Operation not supported");
    }

    Supplier<RedisFuture<Long>> _zrangestorebyscore(K dst, K src, ScoreRange<Double> range) {
        return _zrangestorebyscore(dst, src, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<Long> zrangestorebyscore(K dst, K src, ScoreRange<Double> range) {
        return zrangestorebyscore(dst, src, range, DEFAULT_INSTANCE_RANGE);
    }

    @Override
    public Uni<Long> zrank(K key, V value) {
        return LettuceResult.toUni(_zrank(key, value));
    }

    Supplier<RedisFuture<Long>> _zrank(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> sortedSet.zrank(key, value);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> zrem(K key, V... values) {
        return LettuceResult.toUni(_zrem(key, values)).map(Long::intValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zrem(K key, V... values) {
        nonNull(key, "key");
        notNullOrEmpty(values, "values");
        doesNotContainNull(values, "values");
        return () -> sortedSet.zrem(key, values);
    }

    @Override
    public Uni<Long> zremrangebylex(K key, Range<String> range) {
        return LettuceResult.toUni(_zremrangebylex(key, range));
    }

    Supplier<RedisFuture<Long>> _zremrangebylex(K key, Range<String> range) {
        nonNull(key, "key");
        nonNull(range, "range");
        requireStringMembersFor("zremrangebylex");
        @SuppressWarnings("unchecked")
        io.lettuce.core.Range<V> lettuceRange = (io.lettuce.core.Range<V>) LettuceSortedSetCommandsConverters
                .toLettuceLexRange(range);
        return () -> sortedSet.zremrangebylex(key, lettuceRange);
    }

    @Override
    public Uni<Long> zremrangebyrank(K key, long start, long stop) {
        return LettuceResult.toUni(_zremrangebyrank(key, start, stop));
    }

    Supplier<RedisFuture<Long>> _zremrangebyrank(K key, long start, long stop) {
        nonNull(key, "key");
        return () -> sortedSet.zremrangebyrank(key, start, stop);
    }

    @Override
    public Uni<Long> zremrangebyscore(K key, ScoreRange<Double> range) {
        return LettuceResult.toUni(_zremrangebyscore(key, range));
    }

    Supplier<RedisFuture<Long>> _zremrangebyscore(K key, ScoreRange<Double> range) {
        nonNull(key, "key");
        nonNull(range, "range");
        io.lettuce.core.Range<Number> lettuceRange = LettuceSortedSetCommandsConverters.toLettuceScoreRange(range);
        return () -> sortedSet.zremrangebyscore(key, lettuceRange);
    }

    @Override
    public Uni<Long> zrevrank(K key, V value) {
        return LettuceResult.toUni(_zrevrank(key, value));
    }

    Supplier<RedisFuture<Long>> _zrevrank(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> sortedSet.zrevrank(key, value);
    }

    @Override
    public ReactiveZScanCursor<V> zscan(K key) {
        nonNull(key, "key");
        return new LettuceReactiveZScanCursorImpl<>(sortedSet, key);
    }

    @Override
    public ReactiveZScanCursor<V> zscan(K key, ScanArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        return new LettuceReactiveZScanCursorImpl<>(sortedSet, key,
                LettuceSortedSetCommandsConverters.toLettuceScanArgs(args));
    }

    @Override
    public Uni<Double> zscore(K key, V value) {
        return LettuceResult.toUni(_zscore(key, value));
    }

    Supplier<RedisFuture<Double>> _zscore(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> sortedSet.zscore(key, value);
    }

    @SafeVarargs
    @Override
    public final Uni<List<V>> zunion(ZAggregateArgs args, K... keys) {
        return LettuceResult.toUni(_zunion(args, keys)).map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<V>>> _zunion(ZAggregateArgs args, K... keys) {
        nonNull(args, "args");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        io.lettuce.core.ZAggregateArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(args);
        return () -> sortedSet.zunion(lettuceArgs, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<V>> zunion(K... keys) {
        return zunion(DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<V>>> _zunion(K... keys) {
        return _zunion(DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zunionWithScores(K... keys) {
        return zunionWithScores(DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zunionWithScores(K... keys) {
        return _zunionWithScores(DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zunionWithScores(ZAggregateArgs args, K... keys) {
        return LettuceResult.toUni(_zunionWithScores(args, keys))
                .map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zunionWithScores(ZAggregateArgs args, K... keys) {
        nonNull(args, "args");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        io.lettuce.core.ZAggregateArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(args);
        return () -> sortedSet.zunionWithScores(lettuceArgs, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> zunionstore(K destination, ZAggregateArgs args, K... keys) {
        return LettuceResult.toUni(_zunionstore(destination, args, keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zunionstore(K destination, ZAggregateArgs args, K... keys) {
        nonNull(destination, "destination");
        nonNull(args, "args");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        io.lettuce.core.ZStoreArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZStoreArgs(args);
        return () -> sortedSet.zunionstore(destination, lettuceArgs, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> zunionstore(K destination, K... keys) {
        return zunionstore(destination, DEFAULT_INSTANCE_AGG, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zunionstore(K destination, K... keys) {
        return _zunionstore(destination, DEFAULT_INSTANCE_AGG, keys);
    }

    @Override
    public Uni<List<V>> sort(K key) {
        return sort(key, new SortArgs());
    }

    @Override
    public Uni<List<V>> sort(K key, SortArgs sortArguments) {
        return LettuceResult.toUni(_sort(key, sortArguments)).map(LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _sort(K key, SortArgs sortArguments) {
        nonNull(key, "key");
        nonNull(sortArguments, "sortArguments");
        io.lettuce.core.SortArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceSortArgs(sortArguments);
        return () -> sortable.sort(key, lettuceArgs);
    }

    @Override
    public Uni<Long> sortAndStore(K key, K destination, SortArgs sortArguments) {
        return LettuceResult.toUni(_sortAndStore(key, destination, sortArguments));
    }

    Supplier<RedisFuture<Long>> _sortAndStore(K key, K destination, SortArgs args) {
        nonNull(key, "key");
        nonNull(destination, "destination");
        nonNull(args, "args");
        io.lettuce.core.SortArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceSortArgs(args);
        return () -> sortable.sortStore(key, lettuceArgs, destination);
    }

    @Override
    public Uni<Long> sortAndStore(K key, K destination) {
        return sortAndStore(key, destination, new SortArgs());
    }

    private void requireStringMembersFor(String command) {
        if (!String.class.equals(valueType)) {
            throw new IllegalArgumentException("The Lettuce backend can only run `" + command
                    + "` on a sorted set whose member type is `java.lang.String`, because Lettuce encodes the"
                    + " boundaries of a lexicographical range with the connection codec's value codec. Got member"
                    + " type `" + valueType.getTypeName() + "`. Use quarkus.redis.backend=vertx for other member"
                    + " types.");
        }
    }

    static <V> ScoredValue<V> toScoredValue(io.lettuce.core.ScoredValue<V> value) {
        if (value == null || !value.hasValue()) {
            return null;
        }
        return ScoredValue.of(value.getValue(), value.getScore());
    }

    static <V> ScoredValue<V> poppedOrEmpty(io.lettuce.core.ScoredValue<V> value) {
        ScoredValue<V> converted = toScoredValue(value);
        if (converted == null) {
            return ScoredValue.empty();
        }
        return converted;
    }

    static <V> List<ScoredValue<V>> toScoredValues(List<io.lettuce.core.ScoredValue<V>> values) {
        if (values == null) {
            return List.of();
        }
        List<ScoredValue<V>> result = new ArrayList<>(values.size());
        for (io.lettuce.core.ScoredValue<V> value : values) {
            result.add(toScoredValue(value));
        }
        return result;
    }

    static <K, V> ScoredValue<V> popped(io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>> result) {
        if (result == null || !result.hasValue()) {
            return null;
        }
        return toScoredValue(result.getValue());
    }

    static <K, V> List<ScoredValue<V>> poppedList(
            io.lettuce.core.KeyValue<K, List<io.lettuce.core.ScoredValue<V>>> result) {
        if (result == null || !result.hasValue()) {
            return Collections.emptyList();
        }
        return toScoredValues(result.getValue());
    }

    static <K, V> KeyValue<K, ScoredValue<V>> toKeyValue(
            io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>> result) {
        if (result == null || !result.hasValue()) {
            return null;
        }
        return KeyValue.of(result.getKey(), toScoredValue(result.getValue()));
    }

    static double normalizeScore(double score) {
        if (score == Double.MIN_VALUE) {
            return Double.NEGATIVE_INFINITY;
        }
        if (score == Double.MAX_VALUE) {
            return Double.POSITIVE_INFINITY;
        }
        return score;
    }
}
