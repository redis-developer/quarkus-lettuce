package io.quarkus.redis.runtime.client.lettuce.hash;

import java.util.Map;

import io.quarkus.redis.datasource.hash.ReactiveTransactionalHashCommands;
import io.quarkus.redis.datasource.transactions.ReactiveTransactionalRedisDataSource;
import io.quarkus.redis.runtime.client.lettuce.datasource.LettuceTransactionHolder;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveTransactionalHashCommands}.
 * <p>
 * A thin transactional shell over {@link LettuceReactiveHashCommandsImpl}: each command reuses the
 * non-transactional command-builder seam ({@code reactive._hxxx(...)}) for validation and argument
 * conversion, then registers the {@link io.lettuce.core.RedisFuture} together with a result mapper on
 * the {@link LettuceTransactionHolder}. Each mapper mirrors the {@code .map(...)} of the corresponding
 * non-transactional command, so {@code TransactionResult.get(index)} yields the same Java type that
 * command returns. Where the Vert.x backend's transactional decoder disagrees with its own
 * non-transactional one, this class follows the non-transactional shape.
 *
 * @param <K> the key type
 * @param <F> the field type
 * @param <V> the value type
 */
public class LettuceReactiveTransactionalHashCommandsImpl<K, F, V> implements ReactiveTransactionalHashCommands<K, F, V> {

    private final ReactiveTransactionalRedisDataSource dataSource;
    private final LettuceReactiveHashCommandsImpl<K, F, V> reactive;
    private final LettuceTransactionHolder tx;

    public LettuceReactiveTransactionalHashCommandsImpl(ReactiveTransactionalRedisDataSource dataSource,
            LettuceReactiveHashCommandsImpl<K, F, V> reactive,
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
    public final Uni<Void> hdel(K key, F... fields) {
        return tx.enqueue(reactive._hdel(key, fields), Long::intValue);
    }

    @Override
    public Uni<Void> hexists(K key, F field) {
        return tx.enqueue(reactive._hexists(key, field), v -> v);
    }

    @Override
    public Uni<Void> hget(K key, F field) {
        return tx.enqueue(reactive._hget(key, field), v -> v);
    }

    @Override
    public Uni<Void> hincrby(K key, F field, long amount) {
        return tx.enqueue(reactive._hincrby(key, field, amount), v -> v);
    }

    @Override
    public Uni<Void> hincrbyfloat(K key, F field, double amount) {
        return tx.enqueue(reactive._hincrbyfloat(key, field, amount), v -> v);
    }

    @Override
    public Uni<Void> hgetall(K key) {
        return tx.enqueue(reactive._hgetall(key), v -> v);
    }

    @Override
    public Uni<Void> hkeys(K key) {
        return tx.enqueue(reactive._hkeys(key), v -> v);
    }

    @Override
    public Uni<Void> hlen(K key) {
        return tx.enqueue(reactive._hlen(key), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> hmget(K key, F... fields) {
        return tx.enqueue(reactive._hmget(key, fields), reactive::toMap);
    }

    @Deprecated
    @Override
    public Uni<Void> hmset(K key, Map<F, V> map) {
        return tx.enqueue(reactive._hmset(key, map), v -> null);
    }

    @Override
    public Uni<Void> hrandfield(K key) {
        return tx.enqueue(reactive._hrandfield(key), v -> v);
    }

    @Override
    public Uni<Void> hrandfield(K key, long count) {
        return tx.enqueue(reactive._hrandfield(key, count), v -> v);
    }

    @Override
    public Uni<Void> hrandfieldWithValues(K key, long count) {
        return tx.enqueue(reactive._hrandfieldWithValues(key, count), reactive::toMap);
    }

    @Override
    public Uni<Void> hset(K key, F field, V value) {
        return tx.enqueue(reactive._hset(key, field, value), v -> v);
    }

    @Override
    public Uni<Void> hset(K key, Map<F, V> map) {
        return tx.enqueue(reactive._hset(key, map), v -> v);
    }

    @Override
    public Uni<Void> hsetnx(K key, F field, V value) {
        return tx.enqueue(reactive._hsetnx(key, field, value), v -> v);
    }

    @Override
    public Uni<Void> hstrlen(K key, F field) {
        return tx.enqueue(reactive._hstrlen(key, field), v -> v);
    }

    @Override
    public Uni<Void> hvals(K key) {
        return tx.enqueue(reactive._hvals(key), v -> v);
    }
}
