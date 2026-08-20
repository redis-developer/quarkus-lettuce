package io.quarkus.redis.runtime.client.lettuce.set;

import io.quarkus.redis.datasource.set.ReactiveTransactionalSetCommands;
import io.quarkus.redis.datasource.transactions.ReactiveTransactionalRedisDataSource;
import io.quarkus.redis.runtime.client.lettuce.datasource.LettuceTransactionHolder;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveTransactionalSetCommands}.
 * <p>
 * A thin transactional shell over {@link LettuceReactiveSetCommandsImpl}: each command reuses the
 * non-transactional command-builder seam ({@code reactive._sxxx(...)}) for validation and argument
 * conversion, then registers the {@link io.lettuce.core.RedisFuture} together with a result mapper on
 * the {@link LettuceTransactionHolder}.
 *
 * @param <K> the key type
 * @param <V> the member type
 */
public class LettuceReactiveTransactionalSetCommandsImpl<K, V> implements ReactiveTransactionalSetCommands<K, V> {

    private final ReactiveTransactionalRedisDataSource dataSource;
    private final LettuceReactiveSetCommandsImpl<K, V> reactive;
    private final LettuceTransactionHolder tx;

    public LettuceReactiveTransactionalSetCommandsImpl(ReactiveTransactionalRedisDataSource dataSource,
            LettuceReactiveSetCommandsImpl<K, V> reactive,
            LettuceTransactionHolder tx) {
        this.dataSource = dataSource;
        this.reactive = reactive;
        this.tx = tx;
    }

    @Override
    public ReactiveTransactionalRedisDataSource getDataSource() {
        return dataSource;
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sadd(K key, V... values) {
        return tx.enqueue(reactive._sadd(key, values), Long::intValue);
    }

    @Override
    public Uni<Void> scard(K key) {
        return tx.enqueue(reactive._scard(key), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sdiff(K... keys) {
        return tx.enqueue(reactive._sdiff(keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sdiffstore(K destination, K... keys) {
        return tx.enqueue(reactive._sdiffstore(destination, keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sinter(K... keys) {
        return tx.enqueue(reactive._sinter(keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sintercard(K... keys) {
        return tx.enqueue(reactive._sintercard(keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sintercard(int limit, K... keys) {
        return tx.enqueue(reactive._sintercard(limit, keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sinterstore(K destination, K... keys) {
        return tx.enqueue(reactive._sinterstore(destination, keys), v -> v);
    }

    @Override
    public Uni<Void> sismember(K key, V member) {
        return tx.enqueue(reactive._sismember(key, member), v -> v);
    }

    @Override
    public Uni<Void> smembers(K key) {
        return tx.enqueue(reactive._smembers(key), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> smismember(K key, V... members) {
        return tx.enqueue(reactive._smismember(key, members), LettuceReactiveSetCommandsImpl::orEmpty);
    }

    @Override
    public Uni<Void> smove(K source, K destination, V member) {
        return tx.enqueue(reactive._smove(source, destination, member), v -> v);
    }

    @Override
    public Uni<Void> spop(K key) {
        return tx.enqueue(reactive._spop(key), v -> v);
    }

    @Override
    public Uni<Void> spop(K key, int count) {
        return tx.enqueue(reactive._spop(key, count), v -> v);
    }

    @Override
    public Uni<Void> srandmember(K key) {
        return tx.enqueue(reactive._srandmember(key), v -> v);
    }

    @Override
    public Uni<Void> srandmember(K key, int count) {
        return tx.enqueue(reactive._srandmember(key, count), LettuceReactiveSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> srem(K key, V... members) {
        return tx.enqueue(reactive._srem(key, members), Long::intValue);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sunion(K... keys) {
        return tx.enqueue(reactive._sunion(keys), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> sunionstore(K destination, K... keys) {
        return tx.enqueue(reactive._sunionstore(destination, keys), v -> v);
    }
}
