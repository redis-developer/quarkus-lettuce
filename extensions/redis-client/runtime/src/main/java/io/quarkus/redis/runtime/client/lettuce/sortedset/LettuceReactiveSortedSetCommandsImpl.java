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

import io.lettuce.core.Limit;
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
import io.quarkus.redis.runtime.client.lettuce.sortedset.LettuceSortedSetCommandsConverters.RangeOptions;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveSortedSetCommands}, on top of
 * {@link RedisSortedSetAsyncCommands} plus {@link RedisKeyAsyncCommands} for {@code SORT}.
 * <p>
 * Where the Vert.x backend builds one command and appends the {@code REV} keyword, Lettuce exposes a
 * separate reversed method per command ({@code zrevrange}, {@code zrevrangebylex}, ...). Those methods
 * emit the range boundaries in the opposite order, so a reversed range is handed over with its
 * boundaries swapped wherever needed to reproduce the exact bytes the Vert.x backend sends — see
 * {@link LettuceSortedSetCommandsConverters#toLettuceScoreRange}.
 * <p>
 * Deviations from the Vert.x backend:
 * <ul>
 * <li><strong>Lexicographical commands require a {@code String} member type.</strong> Lettuce takes the
 * boundaries of a lexicographical range as member values and encodes them with the connection codec's
 * value codec, which only reproduces the raw boundary bytes the Vert.x backend sends when the member
 * type is {@code String}. {@code ZLEXCOUNT}, {@code ZRANGEBYLEX}, {@code ZREMRANGEBYLEX} and
 * {@code ZRANGESTORE ... BYLEX} therefore reject any other member type with an
 * {@link IllegalArgumentException} pointing at {@code quarkus.redis.backend=vertx}, see
 * {@link #requireStringMembersFor}.</li>
 * <li><strong>{@code limit} is rejected on the index-based {@code ZRANGE} / {@code ZRANGESTORE}.</strong>
 * Redis only accepts {@code LIMIT} together with {@code BYSCORE} or {@code BYLEX}, and Lettuce's
 * index-based methods take no {@link Limit}. The Vert.x backend forwards the option and surfaces the
 * server's error; this backend fails the {@code Uni} on subscription instead, see
 * {@link #limitNotSupported}.</li>
 * <li>The multi-key {@code ZDIFF*}, {@code ZINTER*} commands reject fewer than two keys, as the Vert.x
 * backend does, and — like it — surface that failure on subscription rather than throwing from the call
 * itself, see {@link #requireAtLeastTwoKeys}. {@code ZUNION*} has no such restriction, again matching
 * the Vert.x backend.</li>
 * <li>Scores equal to {@link Double#MIN_VALUE} or {@link Double#MAX_VALUE} are sent as {@code -inf} and
 * {@code +inf}, as the Vert.x backend does, see {@link #normalizeScore}.</li>
 * <li>A reversed {@code ZRANGESTORE ... BYLEX} whose range has an unbounded boundary sends a range that
 * is empty by construction instead of the Vert.x backend's bytes, which Lettuce cannot reproduce. The
 * observable result is the same — such a query selects nothing under the Vert.x backend either — see
 * {@link LettuceSortedSetCommandsConverters#toLettuceLexRange}.</li>
 * <li>Validation uses the parameter names of the Vert.x backend, which for some commands differ from the
 * public API's: members are validated as {@code value} / {@code values}.</li>
 * <li>A {@code nil} list reply decodes to an empty list, see {@link #orEmpty}.</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the type of the scored member
 */
public class LettuceReactiveSortedSetCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveSortedSetCommands<K, V> {

    private static final ZAddArgs DEFAULT_ADD_ARGS = new ZAddArgs();
    private static final ZAggregateArgs DEFAULT_AGGREGATE_ARGS = new ZAggregateArgs();
    private static final ZRangeArgs DEFAULT_RANGE_ARGS = new ZRangeArgs();
    private static final SortArgs DEFAULT_SORT_ARGS = new SortArgs();

    private final ReactiveRedisDataSource dataSource;

    /**
     * The member type, kept to enforce the {@code String} requirement of the lexicographical commands.
     */
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
        return zadd(key, DEFAULT_ADD_ARGS, score, value);
    }

    @Override
    public Uni<Integer> zadd(K key, Map<V, Double> items) {
        return zadd(key, DEFAULT_ADD_ARGS, items);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> zadd(K key, ScoredValue<V>... items) {
        return zadd(key, DEFAULT_ADD_ARGS, items);
    }

    @Override
    public Uni<Boolean> zadd(K key, ZAddArgs args, double score, V value) {
        return LettuceResult.toUni(_zadd(key, args, score, value)).map(LettuceReactiveSortedSetCommandsImpl::addedOne);
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
        return _zadd(key, DEFAULT_ADD_ARGS, score, value);
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
        io.lettuce.core.ScoredValue<V>[] array = toArray(entries);
        return () -> sortedSet.zadd(key, lettuceArgs, array);
    }

    Supplier<RedisFuture<Long>> _zadd(K key, Map<V, Double> items) {
        return _zadd(key, DEFAULT_ADD_ARGS, items);
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
        io.lettuce.core.ScoredValue<V>[] array = toArray(entries);
        return () -> sortedSet.zadd(key, lettuceArgs, array);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zadd(K key, ScoredValue<V>... items) {
        return _zadd(key, DEFAULT_ADD_ARGS, items);
    }

    @Override
    public Uni<Double> zaddincr(K key, double score, V value) {
        return zaddincr(key, DEFAULT_ADD_ARGS, score, value);
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
        return _zaddincr(key, DEFAULT_ADD_ARGS, score, value);
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
        io.lettuce.core.Range<Number> lettuceRange = LettuceSortedSetCommandsConverters.toLettuceScoreRange(range, false);
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
        return requireAtLeastTwoKeys(keys.length, () -> sortedSet.zdiff(keys));
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
        return requireAtLeastTwoKeys(keys.length, () -> sortedSet.zdiffWithScores(keys));
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
        return requireAtLeastTwoKeys(keys.length, () -> sortedSet.zdiffstore(destination, keys));
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
        io.lettuce.core.ZAggregateArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZAggregateArgs(args);
        return requireAtLeastTwoKeys(keys.length, () -> sortedSet.zinter(lettuceArgs, keys));
    }

    @SafeVarargs
    @Override
    public final Uni<List<V>> zinter(K... keys) {
        return zinter(DEFAULT_AGGREGATE_ARGS, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<V>>> _zinter(K... keys) {
        return _zinter(DEFAULT_AGGREGATE_ARGS, keys);
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
        io.lettuce.core.ZAggregateArgs lettuceArgs = LettuceSortedSetCommandsConverters
                .toLettuceZAggregateArgs(arguments);
        return requireAtLeastTwoKeys(keys.length, () -> sortedSet.zinterWithScores(lettuceArgs, keys));
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zinterWithScores(K... keys) {
        return zinterWithScores(DEFAULT_AGGREGATE_ARGS, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zinterWithScores(K... keys) {
        return _zinterWithScores(DEFAULT_AGGREGATE_ARGS, keys);
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
        return requireAtLeastTwoKeys(keys.length, () -> sortedSet.zintercard(keys));
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
            // The Vert.x backend returns its failure before validating `limit`; match that order.
            return atLeastTwoKeysFailure();
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
        io.lettuce.core.ZStoreArgs lettuceArgs = LettuceSortedSetCommandsConverters.toLettuceZStoreArgs(arguments);
        return requireAtLeastTwoKeys(keys.length, () -> sortedSet.zinterstore(destination, lettuceArgs, keys));
    }

    @SafeVarargs
    @Override
    public final Uni<Long> zinterstore(K destination, K... keys) {
        return zinterstore(destination, DEFAULT_AGGREGATE_ARGS, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zinterstore(K destination, K... keys) {
        return _zinterstore(destination, DEFAULT_AGGREGATE_ARGS, keys);
    }

    @Override
    public Uni<Long> zlexcount(K key, Range<String> range) {
        return LettuceResult.toUni(_zlexcount(key, range));
    }

    Supplier<RedisFuture<Long>> _zlexcount(K key, Range<String> range) {
        nonNull(key, "key");
        nonNull(range, "range");
        requireStringMembersFor("zlexcount");
        io.lettuce.core.Range<V> lettuceRange = asMemberRange(
                LettuceSortedSetCommandsConverters.toLettuceLexRange(range, false));
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
        nonNull(keys, "keys");
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
        nonNull(keys, "keys");
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
        RangeOptions options = LettuceSortedSetCommandsConverters.toLettuceRangeOptions(args);
        if (options.hasLimit()) {
            return limitNotSupported("zrange");
        }
        if (options.isReverse()) {
            return () -> sortedSet.zrevrange(key, start, stop);
        }
        return () -> sortedSet.zrange(key, start, stop);
    }

    Supplier<RedisFuture<List<V>>> _zrange(K key, long start, long stop) {
        return _zrange(key, start, stop, DEFAULT_RANGE_ARGS);
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
        RangeOptions options = LettuceSortedSetCommandsConverters.toLettuceRangeOptions(args);
        if (options.hasLimit()) {
            return limitNotSupported("zrangeWithScores");
        }
        if (options.isReverse()) {
            return () -> sortedSet.zrevrangeWithScores(key, start, stop);
        }
        return () -> sortedSet.zrangeWithScores(key, start, stop);
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zrangeWithScores(K key, long start, long stop) {
        return _zrangeWithScores(key, start, stop, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<List<V>> zrange(K key, long start, long stop) {
        return zrange(key, start, stop, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zrangeWithScores(K key, long start, long stop) {
        return zrangeWithScores(key, start, stop, DEFAULT_RANGE_ARGS);
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
        RangeOptions options = LettuceSortedSetCommandsConverters.toLettuceRangeOptions(args);
        // ZREVRANGEBYLEX already emits the upper boundary first, exactly as the Vert.x backend does for a
        // reversed lexicographical read, so the boundaries are handed over unswapped.
        io.lettuce.core.Range<V> lettuceRange = asMemberRange(
                LettuceSortedSetCommandsConverters.toLettuceLexRange(range, false));
        Limit limit = options.limit();
        if (options.isReverse()) {
            return () -> sortedSet.zrevrangebylex(key, lettuceRange, limit);
        }
        return () -> sortedSet.zrangebylex(key, lettuceRange, limit);
    }

    Supplier<RedisFuture<List<V>>> _zrangebylex(K key, Range<String> range) {
        return _zrangebylex(key, range, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<List<V>> zrangebylex(K key, Range<String> range) {
        return zrangebylex(key, range, DEFAULT_RANGE_ARGS);
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
        RangeOptions options = LettuceSortedSetCommandsConverters.toLettuceRangeOptions(args);
        // The Vert.x backend emits a reversed BYSCORE read in declaration order — the caller supplies
        // (max, min) — except for a fully unbounded range, whose boundaries it switches itself. Lettuce
        // already emits the upper boundary first, so swap exactly when Vert.x does not switch.
        io.lettuce.core.Range<Number> lettuceRange = LettuceSortedSetCommandsConverters.toLettuceScoreRange(range,
                options.isReverse() && !range.isUnbounded());
        Limit limit = options.limit();
        if (options.isReverse()) {
            return () -> sortedSet.zrevrangebyscore(key, lettuceRange, limit);
        }
        return () -> sortedSet.zrangebyscore(key, lettuceRange, limit);
    }

    Supplier<RedisFuture<List<V>>> _zrangebyscore(K key, ScoreRange<Double> range) {
        return _zrangebyscore(key, range, DEFAULT_RANGE_ARGS);
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
        RangeOptions options = LettuceSortedSetCommandsConverters.toLettuceRangeOptions(args);
        // See _zrangebyscore for why a fully unbounded reversed range is not swapped.
        io.lettuce.core.Range<Number> lettuceRange = LettuceSortedSetCommandsConverters.toLettuceScoreRange(range,
                options.isReverse() && !range.isUnbounded());
        Limit limit = options.limit();
        if (options.isReverse()) {
            return () -> sortedSet.zrevrangebyscoreWithScores(key, lettuceRange, limit);
        }
        return () -> sortedSet.zrangebyscoreWithScores(key, lettuceRange, limit);
    }

    Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zrangebyscoreWithScores(K key,
            ScoreRange<Double> range) {
        return _zrangebyscoreWithScores(key, range, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<List<V>> zrangebyscore(K key, ScoreRange<Double> range) {
        return zrangebyscore(key, range, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<List<ScoredValue<V>>> zrangebyscoreWithScores(K key, ScoreRange<Double> range) {
        return zrangebyscoreWithScores(key, range, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<Long> zrangestore(K dst, K src, long min, long max, ZRangeArgs args) {
        return LettuceResult.toUni(_zrangestore(dst, src, min, max, args));
    }

    Supplier<RedisFuture<Long>> _zrangestore(K dst, K src, long min, long max, ZRangeArgs args) {
        nonNull(dst, "dst");
        nonNull(src, "src");
        nonNull(args, "args");
        RangeOptions options = LettuceSortedSetCommandsConverters.toLettuceRangeOptions(args);
        if (options.hasLimit()) {
            return limitNotSupported("zrangestore");
        }
        io.lettuce.core.Range<Long> indexes = io.lettuce.core.Range.create(min, max);
        if (options.isReverse()) {
            return () -> sortedSet.zrevrangestore(dst, src, indexes);
        }
        return () -> sortedSet.zrangestore(dst, src, indexes);
    }

    Supplier<RedisFuture<Long>> _zrangestore(K dst, K src, long min, long max) {
        return _zrangestore(dst, src, min, max, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<Long> zrangestore(K dst, K src, long min, long max) {
        return zrangestore(dst, src, min, max, DEFAULT_RANGE_ARGS);
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
        RangeOptions options = LettuceSortedSetCommandsConverters.toLettuceRangeOptions(args);
        // Unlike its reversed read counterpart, the Vert.x backend does not switch the boundaries of a
        // reversed ZRANGESTORE ... BYLEX, so they are swapped here to compensate for Lettuce emitting
        // the upper boundary first.
        io.lettuce.core.Range<V> lettuceRange = asMemberRange(
                LettuceSortedSetCommandsConverters.toLettuceLexRange(range, options.isReverse()));
        Limit limit = options.limit();
        if (options.isReverse()) {
            return () -> sortedSet.zrevrangestorebylex(dst, src, lettuceRange, limit);
        }
        return () -> sortedSet.zrangestorebylex(dst, src, lettuceRange, limit);
    }

    Supplier<RedisFuture<Long>> _zrangestorebylex(K dst, K src, Range<String> range) {
        return _zrangestorebylex(dst, src, range, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<Long> zrangestorebylex(K dst, K src, Range<String> range) {
        return zrangestorebylex(dst, src, range, DEFAULT_RANGE_ARGS);
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
        RangeOptions options = LettuceSortedSetCommandsConverters.toLettuceRangeOptions(args);
        io.lettuce.core.Range<Number> lettuceRange = LettuceSortedSetCommandsConverters.toLettuceScoreRange(range,
                options.isReverse());
        Limit limit = options.limit();
        if (options.isReverse()) {
            return () -> sortedSet.zrevrangestorebyscore(dst, src, lettuceRange, limit);
        }
        return () -> sortedSet.zrangestorebyscore(dst, src, lettuceRange, limit);
    }

    Supplier<RedisFuture<Long>> _zrangestorebyscore(K dst, K src, ScoreRange<Double> range) {
        return _zrangestorebyscore(dst, src, range, DEFAULT_RANGE_ARGS);
    }

    @Override
    public Uni<Long> zrangestorebyscore(K dst, K src, ScoreRange<Double> range) {
        return zrangestorebyscore(dst, src, range, DEFAULT_RANGE_ARGS);
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
        io.lettuce.core.Range<V> lettuceRange = asMemberRange(
                LettuceSortedSetCommandsConverters.toLettuceLexRange(range, false));
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
        io.lettuce.core.Range<Number> lettuceRange = LettuceSortedSetCommandsConverters.toLettuceScoreRange(range, false);
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
        return zunion(DEFAULT_AGGREGATE_ARGS, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<V>>> _zunion(K... keys) {
        return _zunion(DEFAULT_AGGREGATE_ARGS, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<ScoredValue<V>>> zunionWithScores(K... keys) {
        return zunionWithScores(DEFAULT_AGGREGATE_ARGS, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<io.lettuce.core.ScoredValue<V>>>> _zunionWithScores(K... keys) {
        return _zunionWithScores(DEFAULT_AGGREGATE_ARGS, keys);
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
        return zunionstore(destination, DEFAULT_AGGREGATE_ARGS, keys);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _zunionstore(K destination, K... keys) {
        return _zunionstore(destination, DEFAULT_AGGREGATE_ARGS, keys);
    }

    @Override
    public Uni<List<V>> sort(K key) {
        return sort(key, DEFAULT_SORT_ARGS);
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
        return sortAndStore(key, destination, DEFAULT_SORT_ARGS);
    }

    /**
     * Rejects a lexicographical command when the member type is not {@code String}, see the class Javadoc.
     */
    private void requireStringMembersFor(String command) {
        if (!String.class.equals(valueType)) {
            throw new IllegalArgumentException("The Lettuce backend can only run `" + command
                    + "` on a sorted set whose member type is `java.lang.String`, because Lettuce encodes the"
                    + " boundaries of a lexicographical range with the connection codec's value codec. Got member"
                    + " type `" + valueType.getTypeName() + "`. Use quarkus.redis.backend=vertx for other member"
                    + " types.");
        }
    }

    /**
     * Retypes a lexicographical range to the member type. The cast is safe because every caller has passed
     * {@link #requireStringMembersFor} first, which rejects a member type other than {@code String}.
     */
    @SuppressWarnings("unchecked")
    private io.lettuce.core.Range<V> asMemberRange(io.lettuce.core.Range<String> range) {
        return (io.lettuce.core.Range<V>) (io.lettuce.core.Range<?>) range;
    }

    /**
     * Guards a multi-key command with the Vert.x backend's "at least 2 keys" rule. That backend fails
     * lazily — the caller gets a failed {@code Uni}, not an exception — so the check runs inside the
     * returned supplier.
     */
    private static <T> Supplier<RedisFuture<T>> requireAtLeastTwoKeys(int keyCount, Supplier<RedisFuture<T>> command) {
        return keyCount < 2 ? atLeastTwoKeysFailure() : command;
    }

    private static <T> Supplier<RedisFuture<T>> atLeastTwoKeysFailure() {
        return () -> {
            throw new IllegalArgumentException("Need at least two keys");
        };
    }

    /**
     * Rejects a {@code limit} the index-based commands cannot carry, on subscription, see the class Javadoc.
     */
    private static <T> Supplier<RedisFuture<T>> limitNotSupported(String command) {
        return () -> {
            throw new IllegalArgumentException("`args` must not carry a `limit` for `" + command
                    + "`: LIMIT is only supported in combination with BYSCORE or BYLEX");
        };
    }

    /**
     * Maps the {@code ZADD} reply of a single-member add to a boolean, as the Vert.x backend does.
     */
    static Boolean addedOne(Long added) {
        return added != null && added == 1L;
    }

    /**
     * A {@code nil} {@code ZCARD} reply decodes to zero, as in the Vert.x backend.
     */
    static Long orZero(Long value) {
        return value == null ? 0L : value;
    }

    /**
     * A {@code nil} multi-bulk reply decodes to an empty list, as in the Vert.x backend.
     */
    static <T> List<T> orEmpty(List<T> list) {
        return list == null ? Collections.emptyList() : list;
    }

    /**
     * Converts one Lettuce scored value; a missing value decodes to {@code null}.
     */
    static <V> ScoredValue<V> toScoredValue(io.lettuce.core.ScoredValue<V> value) {
        if (value == null || !value.hasValue()) {
            return null;
        }
        return ScoredValue.of(value.getValue(), value.getScore());
    }

    /**
     * As {@link #toScoredValue}, but a missing value decodes to {@link ScoredValue#empty()} — the shape
     * {@code ZPOPMIN}, {@code ZPOPMAX} and {@code ZRANDMEMBER ... WITHSCORES} return in the Vert.x backend.
     */
    static <V> ScoredValue<V> poppedOrEmpty(io.lettuce.core.ScoredValue<V> value) {
        ScoredValue<V> converted = toScoredValue(value);
        return converted == null ? ScoredValue.empty() : converted;
    }

    static <V> List<ScoredValue<V>> toScoredValues(List<io.lettuce.core.ScoredValue<V>> values) {
        if (values == null) {
            return Collections.emptyList();
        }
        List<ScoredValue<V>> result = new ArrayList<>(values.size());
        for (io.lettuce.core.ScoredValue<V> value : values) {
            result.add(toScoredValue(value));
        }
        return result;
    }

    /**
     * Drops the key Lettuce reports alongside a {@code ZMPOP} / {@code BZMPOP} result — the Quarkus API
     * exposes the popped member only. An exhausted or timed-out pop decodes to {@code null}.
     */
    static <K, V> ScoredValue<V> popped(io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>> result) {
        if (result == null || !result.hasValue()) {
            return null;
        }
        return toScoredValue(result.getValue());
    }

    /**
     * As {@link #popped}, for the counted variants, which return an empty list when nothing was popped.
     */
    static <K, V> List<ScoredValue<V>> poppedList(
            io.lettuce.core.KeyValue<K, List<io.lettuce.core.ScoredValue<V>>> result) {
        if (result == null || !result.hasValue()) {
            return Collections.emptyList();
        }
        return toScoredValues(result.getValue());
    }

    /**
     * Converts the {@code BZPOPMIN} / {@code BZPOPMAX} reply; a timeout decodes to {@code null}.
     */
    static <K, V> KeyValue<K, ScoredValue<V>> toKeyValue(
            io.lettuce.core.KeyValue<K, io.lettuce.core.ScoredValue<V>> result) {
        if (result == null || !result.hasValue()) {
            return null;
        }
        return KeyValue.of(result.getKey(), toScoredValue(result.getValue()));
    }

    /**
     * Reproduces the Vert.x backend's score formatting: it sends {@link Double#MIN_VALUE} as {@code -inf}
     * and {@link Double#MAX_VALUE} as {@code +inf}, on top of the infinities Lettuce already formats that
     * way.
     */
    static double normalizeScore(double score) {
        if (score == Double.MIN_VALUE) {
            return Double.NEGATIVE_INFINITY;
        }
        if (score == Double.MAX_VALUE) {
            return Double.POSITIVE_INFINITY;
        }
        return score;
    }

    /**
     * Lettuce's {@code ZADD} takes its members as a vararg array. The cast is safe: the array is created
     * here, holds only the elements of {@code entries}, and never escapes to a caller that could widen it.
     */
    @SuppressWarnings("unchecked")
    private static <V> io.lettuce.core.ScoredValue<V>[] toArray(List<io.lettuce.core.ScoredValue<V>> entries) {
        return entries.toArray(new io.lettuce.core.ScoredValue[0]);
    }
}
