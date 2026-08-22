package io.quarkus.redis.runtime.client.lettuce.sortedset;

import java.time.Duration;
import java.util.Map;

import io.quarkus.redis.datasource.sortedset.Range;
import io.quarkus.redis.datasource.sortedset.ReactiveTransactionalSortedSetCommands;
import io.quarkus.redis.datasource.sortedset.ScoreRange;
import io.quarkus.redis.datasource.sortedset.ScoredValue;
import io.quarkus.redis.datasource.sortedset.ZAddArgs;
import io.quarkus.redis.datasource.sortedset.ZAggregateArgs;
import io.quarkus.redis.datasource.sortedset.ZRangeArgs;
import io.quarkus.redis.datasource.transactions.ReactiveTransactionalRedisDataSource;
import io.quarkus.redis.runtime.client.lettuce.datasource.LettuceTransactionHolder;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveTransactionalSortedSetCommands}.
 * <p>
 * A thin transactional shell over {@link LettuceReactiveSortedSetCommandsImpl}: each command reuses the
 * non-transactional command-builder seam ({@code reactive._zxxx(...)}) for validation and argument
 * conversion, then registers the {@link io.lettuce.core.RedisFuture} together with a result mapper on
 * the {@link LettuceTransactionHolder}.
 * <p>
 * Every mapper mirrors the {@code .map(...)} of the matching non-transactional command, so
 * {@code TransactionResult.get(i)} yields the type that command returns. Where the Vert.x transactional
 * decoder disagrees with its own non-transactional one — it decodes {@code ZADD} with arguments as a
 * plain boolean instead of comparing the reply to {@code 1} — the non-transactional shape wins.
 *
 * @param <K> the key type
 * @param <V> the type of the scored member
 */
public class LettuceReactiveTransactionalSortedSetCommandsImpl<K, V>
        implements ReactiveTransactionalSortedSetCommands<K, V> {

    private final ReactiveTransactionalRedisDataSource dataSource;
    private final LettuceReactiveSortedSetCommandsImpl<K, V> reactive;
    private final LettuceTransactionHolder tx;

    public LettuceReactiveTransactionalSortedSetCommandsImpl(ReactiveTransactionalRedisDataSource dataSource,
            LettuceReactiveSortedSetCommandsImpl<K, V> reactive,
            LettuceTransactionHolder tx) {
        this.dataSource = dataSource;
        this.reactive = reactive;
        this.tx = tx;
    }

    @Override
    public ReactiveTransactionalRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Void> zadd(K key, double score, V member) {
        return tx.enqueue(reactive._zadd(key, score, member), LettuceReactiveSortedSetCommandsImpl::asBoolean);
    }

    @Override
    public Uni<Void> zadd(K key, Map<V, Double> items) {
        return tx.enqueue(reactive._zadd(key, items), Long::intValue);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zadd(K key, ScoredValue<V>... items) {
        return tx.enqueue(reactive._zadd(key, items), Long::intValue);
    }

    @Override
    public Uni<Void> zadd(K key, ZAddArgs zAddArgs, double score, V member) {
        return tx.enqueue(reactive._zadd(key, zAddArgs, score, member), LettuceReactiveSortedSetCommandsImpl::asBoolean);
    }

    @Override
    public Uni<Void> zadd(K key, ZAddArgs zAddArgs, Map<V, Double> items) {
        return tx.enqueue(reactive._zadd(key, zAddArgs, items), Long::intValue);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zadd(K key, ZAddArgs zAddArgs, ScoredValue<V>... items) {
        return tx.enqueue(reactive._zadd(key, zAddArgs, items), Long::intValue);
    }

    @Override
    public Uni<Void> zaddincr(K key, double score, V member) {
        return tx.enqueue(reactive._zaddincr(key, score, member), v -> v);
    }

    @Override
    public Uni<Void> zaddincr(K key, ZAddArgs zAddArgs, double score, V member) {
        return tx.enqueue(reactive._zaddincr(key, zAddArgs, score, member), v -> v);
    }

    @Override
    public Uni<Void> zcard(K key) {
        return tx.enqueue(reactive._zcard(key), LettuceReactiveSortedSetCommandsImpl::orZero);
    }

    @Override
    public Uni<Void> zcount(K key, ScoreRange<Double> range) {
        return tx.enqueue(reactive._zcount(key, range), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zdiff(K... keys) {
        return tx.enqueue(reactive._zdiff(keys), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zdiffWithScores(K... keys) {
        return tx.enqueue(reactive._zdiffWithScores(keys), LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zdiffstore(K destination, K... keys) {
        return tx.enqueue(reactive._zdiffstore(destination, keys), v -> v);
    }

    @Override
    public Uni<Void> zincrby(K key, double increment, V member) {
        return tx.enqueue(reactive._zincrby(key, increment, member), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zinter(ZAggregateArgs arguments, K... keys) {
        return tx.enqueue(reactive._zinter(arguments, keys), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zinter(K... keys) {
        return tx.enqueue(reactive._zinter(keys), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zinterWithScores(ZAggregateArgs arguments, K... keys) {
        return tx.enqueue(reactive._zinterWithScores(arguments, keys),
                LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zinterWithScores(K... keys) {
        return tx.enqueue(reactive._zinterWithScores(keys), LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zintercard(K... keys) {
        return tx.enqueue(reactive._zintercard(keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zintercard(long limit, K... keys) {
        return tx.enqueue(reactive._zintercard(limit, keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zinterstore(K destination, ZAggregateArgs arguments, K... keys) {
        return tx.enqueue(reactive._zinterstore(destination, arguments, keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zinterstore(K destination, K... keys) {
        return tx.enqueue(reactive._zinterstore(destination, keys), v -> v);
    }

    @Override
    public Uni<Void> zlexcount(K key, Range<String> range) {
        return tx.enqueue(reactive._zlexcount(key, range), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zmpopMin(K... keys) {
        return tx.enqueue(reactive._zmpopMin(keys), LettuceReactiveSortedSetCommandsImpl::popped);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zmpopMin(int count, K... keys) {
        return tx.enqueue(reactive._zmpopMin(count, keys), LettuceReactiveSortedSetCommandsImpl::poppedList);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zmpopMax(K... keys) {
        return tx.enqueue(reactive._zmpopMax(keys), LettuceReactiveSortedSetCommandsImpl::popped);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zmpopMax(int count, K... keys) {
        return tx.enqueue(reactive._zmpopMax(count, keys), LettuceReactiveSortedSetCommandsImpl::poppedList);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> bzmpopMin(Duration timeout, K... keys) {
        return tx.enqueue(reactive._bzmpopMin(timeout, keys), LettuceReactiveSortedSetCommandsImpl::popped);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> bzmpopMin(Duration timeout, int count, K... keys) {
        return tx.enqueue(reactive._bzmpopMin(timeout, count, keys),
                LettuceReactiveSortedSetCommandsImpl::poppedList);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> bzmpopMax(Duration timeout, K... keys) {
        return tx.enqueue(reactive._bzmpopMax(timeout, keys), LettuceReactiveSortedSetCommandsImpl::popped);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> bzmpopMax(Duration timeout, int count, K... keys) {
        return tx.enqueue(reactive._bzmpopMax(timeout, count, keys),
                LettuceReactiveSortedSetCommandsImpl::poppedList);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zmscore(K key, V... members) {
        return tx.enqueue(reactive._zmscore(key, members), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> zpopmax(K key) {
        return tx.enqueue(reactive._zpopmax(key), LettuceReactiveSortedSetCommandsImpl::poppedOrEmpty);
    }

    @Override
    public Uni<Void> zpopmax(K key, int count) {
        return tx.enqueue(reactive._zpopmax(key, count), LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @Override
    public Uni<Void> zpopmin(K key) {
        return tx.enqueue(reactive._zpopmin(key), LettuceReactiveSortedSetCommandsImpl::poppedOrEmpty);
    }

    @Override
    public Uni<Void> zpopmin(K key, int count) {
        return tx.enqueue(reactive._zpopmin(key, count), LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @Override
    public Uni<Void> zrandmember(K key) {
        return tx.enqueue(reactive._zrandmember(key), v -> v);
    }

    @Override
    public Uni<Void> zrandmember(K key, int count) {
        return tx.enqueue(reactive._zrandmember(key, count), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> zrandmemberWithScores(K key) {
        return tx.enqueue(reactive._zrandmemberWithScores(key), LettuceReactiveSortedSetCommandsImpl::poppedOrEmpty);
    }

    @Override
    public Uni<Void> zrandmemberWithScores(K key, int count) {
        return tx.enqueue(reactive._zrandmemberWithScores(key, count),
                LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> bzpopmin(Duration timeout, K... keys) {
        return tx.enqueue(reactive._bzpopmin(timeout, keys), LettuceReactiveSortedSetCommandsImpl::toKeyValue);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> bzpopmax(Duration timeout, K... keys) {
        return tx.enqueue(reactive._bzpopmax(timeout, keys), LettuceReactiveSortedSetCommandsImpl::toKeyValue);
    }

    @Override
    public Uni<Void> zrange(K key, long start, long stop, ZRangeArgs args) {
        return tx.enqueue(reactive._zrange(key, start, stop, args), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> zrangeWithScores(K key, long start, long stop, ZRangeArgs args) {
        return tx.enqueue(reactive._zrangeWithScores(key, start, stop, args),
                LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @Override
    public Uni<Void> zrange(K key, long start, long stop) {
        return tx.enqueue(reactive._zrange(key, start, stop), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> zrangeWithScores(K key, long start, long stop) {
        return tx.enqueue(reactive._zrangeWithScores(key, start, stop),
                LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @Override
    public Uni<Void> zrangebylex(K key, Range<String> range, ZRangeArgs args) {
        return tx.enqueue(reactive._zrangebylex(key, range, args), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> zrangebylex(K key, Range<String> range) {
        return tx.enqueue(reactive._zrangebylex(key, range), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> zrangebyscore(K key, ScoreRange<Double> range, ZRangeArgs args) {
        return tx.enqueue(reactive._zrangebyscore(key, range, args), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> zrangebyscoreWithScores(K key, ScoreRange<Double> range, ZRangeArgs args) {
        return tx.enqueue(reactive._zrangebyscoreWithScores(key, range, args),
                LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @Override
    public Uni<Void> zrangebyscore(K key, ScoreRange<Double> range) {
        return tx.enqueue(reactive._zrangebyscore(key, range), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> zrangebyscoreWithScores(K key, ScoreRange<Double> range) {
        return tx.enqueue(reactive._zrangebyscoreWithScores(key, range),
                LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @Override
    public Uni<Void> zrangestore(K dst, K src, long min, long max, ZRangeArgs args) {
        return tx.enqueue(reactive._zrangestore(dst, src, min, max, args), v -> v);
    }

    @Override
    public Uni<Void> zrangestore(K dst, K src, long min, long max) {
        return tx.enqueue(reactive._zrangestore(dst, src, min, max), v -> v);
    }

    @Override
    public Uni<Void> zrangestorebylex(K dst, K src, Range<String> range, ZRangeArgs args) {
        return tx.enqueue(reactive._zrangestorebylex(dst, src, range, args), v -> v);
    }

    @Override
    public Uni<Void> zrangestorebylex(K dst, K src, Range<String> range) {
        return tx.enqueue(reactive._zrangestorebylex(dst, src, range), v -> v);
    }

    @Override
    public Uni<Void> zrangestorebyscore(K dst, K src, ScoreRange<Double> range, ZRangeArgs args) {
        return tx.enqueue(reactive._zrangestorebyscore(dst, src, range, args), v -> v);
    }

    @Override
    public Uni<Void> zrangestorebyscore(K dst, K src, ScoreRange<Double> range) {
        return tx.enqueue(reactive._zrangestorebyscore(dst, src, range), v -> v);
    }

    @Override
    public Uni<Void> zrank(K key, V member) {
        return tx.enqueue(reactive._zrank(key, member), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zrem(K key, V... members) {
        return tx.enqueue(reactive._zrem(key, members), Long::intValue);
    }

    @Override
    public Uni<Void> zremrangebylex(K key, Range<String> range) {
        return tx.enqueue(reactive._zremrangebylex(key, range), v -> v);
    }

    @Override
    public Uni<Void> zremrangebyrank(K key, long start, long stop) {
        return tx.enqueue(reactive._zremrangebyrank(key, start, stop), v -> v);
    }

    @Override
    public Uni<Void> zremrangebyscore(K key, ScoreRange<Double> range) {
        return tx.enqueue(reactive._zremrangebyscore(key, range), v -> v);
    }

    @Override
    public Uni<Void> zrevrank(K key, V member) {
        return tx.enqueue(reactive._zrevrank(key, member), v -> v);
    }

    @Override
    public Uni<Void> zscore(K key, V member) {
        return tx.enqueue(reactive._zscore(key, member), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zunion(ZAggregateArgs args, K... keys) {
        return tx.enqueue(reactive._zunion(args, keys), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zunion(K... keys) {
        return tx.enqueue(reactive._zunion(keys), LettuceReactiveSortedSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zunionWithScores(ZAggregateArgs args, K... keys) {
        return tx.enqueue(reactive._zunionWithScores(args, keys), LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zunionWithScores(K... keys) {
        return tx.enqueue(reactive._zunionWithScores(keys), LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zunionstore(K destination, ZAggregateArgs args, K... keys) {
        return tx.enqueue(reactive._zunionstore(destination, args, keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> zunionstore(K destination, K... keys) {
        return tx.enqueue(reactive._zunionstore(destination, keys), v -> v);
    }
}
