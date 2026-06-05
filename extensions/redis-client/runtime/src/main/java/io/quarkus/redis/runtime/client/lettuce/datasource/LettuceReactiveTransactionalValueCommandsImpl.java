package io.quarkus.redis.runtime.client.lettuce.datasource;

import java.util.Map;

import io.quarkus.redis.datasource.transactions.ReactiveTransactionalRedisDataSource;
import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.ReactiveTransactionalValueCommands;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.runtime.client.lettuce.value.LettuceReactiveValueCommandsImpl;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveTransactionalValueCommands}.
 * <p>
 * A thin transactional shell over {@link LettuceReactiveValueCommandsImpl}: each command reuses the
 * non-transactional command-builder seam ({@code reactive._xxx(...)}) for validation and argument
 * conversion, then registers the {@link io.lettuce.core.RedisFuture} together with a result mapper on
 * the {@link LettuceTransactionHolder}. The mapper mirrors the {@code .map(...)} of the corresponding
 * non-transactional command, so the assembled {@code TransactionResult.get(index)} yields the same
 * Java type as the Vert.x backend.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LettuceReactiveTransactionalValueCommandsImpl<K, V>
        implements ReactiveTransactionalValueCommands<K, V> {

    private final ReactiveTransactionalRedisDataSource dataSource;
    private final LettuceReactiveValueCommandsImpl<K, V> reactive;
    private final LettuceTransactionHolder tx;

    public LettuceReactiveTransactionalValueCommandsImpl(ReactiveTransactionalRedisDataSource dataSource,
            LettuceReactiveValueCommandsImpl<K, V> reactive, LettuceTransactionHolder tx) {
        this.dataSource = dataSource;
        this.reactive = reactive;
        this.tx = tx;
    }

    @Override
    public ReactiveTransactionalRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Void> append(K key, V value) {
        return tx.enqueue(reactive._append(key, value), v -> v);
    }

    @Override
    public Uni<Void> decr(K key) {
        return tx.enqueue(reactive._decr(key), v -> v);
    }

    @Override
    public Uni<Void> decrby(K key, long amount) {
        return tx.enqueue(reactive._decrby(key, amount), v -> v);
    }

    @Override
    public Uni<Void> get(K key) {
        return tx.enqueue(reactive._get(key), v -> v);
    }

    @Override
    public Uni<Void> getdel(K key) {
        return tx.enqueue(reactive._getdel(key), v -> v);
    }

    @Override
    public Uni<Void> getex(K key, GetExArgs args) {
        return tx.enqueue(reactive._getex(key, args), v -> v);
    }

    @Override
    public Uni<Void> getrange(K key, long start, long end) {
        return tx.enqueue(reactive._getrange(key, start, end), v -> v == null ? null : v.toString());
    }

    @Override
    public Uni<Void> getset(K key, V value) {
        return tx.enqueue(reactive._getset(key, value), v -> v);
    }

    @Override
    public Uni<Void> incr(K key) {
        return tx.enqueue(reactive._incr(key), v -> v);
    }

    @Override
    public Uni<Void> incrby(K key, long amount) {
        return tx.enqueue(reactive._incrby(key, amount), v -> v);
    }

    @Override
    public Uni<Void> incrbyfloat(K key, double amount) {
        return tx.enqueue(reactive._incrbyfloat(key, amount), v -> v);
    }

    @Override
    public Uni<Void> lcs(K key1, K key2) {
        return Uni.createFrom().failure(lcsUnsupported());
    }

    @Override
    public Uni<Void> lcsLength(K key1, K key2) {
        return Uni.createFrom().failure(lcsUnsupported());
    }

    @SafeVarargs
    @Override
    public final Uni<Void> mget(K... keys) {
        return tx.enqueue(reactive._mget(keys), reactive::toOrderedMap);
    }

    @Override
    public Uni<Void> mset(Map<K, V> map) {
        return tx.enqueue(reactive._mset(map), v -> null);
    }

    @Override
    public Uni<Void> msetnx(Map<K, V> map) {
        return tx.enqueue(reactive._msetnx(map), v -> v);
    }

    @Override
    public Uni<Void> psetex(K key, long milliseconds, V value) {
        return tx.enqueue(reactive._psetex(key, milliseconds, value), v -> null);
    }

    @Override
    public Uni<Void> set(K key, V value) {
        return tx.enqueue(reactive._set(key, value), v -> null);
    }

    @Override
    public Uni<Void> set(K key, V value, SetArgs setArgs) {
        return tx.enqueue(reactive._set(key, value, setArgs), v -> null);
    }

    @Override
    public Uni<Void> setAndChanged(K key, V value) {
        return tx.enqueue(reactive._set(key, value), LettuceReactiveValueCommandsImpl::isOk);
    }

    @Override
    public Uni<Void> setAndChanged(K key, V value, SetArgs setArgs) {
        return tx.enqueue(reactive._set(key, value, setArgs), LettuceReactiveValueCommandsImpl::isOk);
    }

    @Override
    public Uni<Void> setGet(K key, V value) {
        return tx.enqueue(reactive._setGet(key, value), v -> v);
    }

    @Override
    public Uni<Void> setGet(K key, V value, SetArgs setArgs) {
        return tx.enqueue(reactive._setGet(key, value, setArgs), v -> v);
    }

    @Override
    public Uni<Void> setex(K key, long seconds, V value) {
        return tx.enqueue(reactive._setex(key, seconds, value), v -> null);
    }

    @Override
    public Uni<Void> setnx(K key, V value) {
        return tx.enqueue(reactive._setnx(key, value), v -> v);
    }

    @Override
    public Uni<Void> setrange(K key, long offset, V value) {
        return tx.enqueue(reactive._setrange(key, offset, value), v -> v);
    }

    @Override
    public Uni<Void> strlen(K key) {
        return tx.enqueue(reactive._strlen(key), v -> v);
    }

    private static UnsupportedOperationException lcsUnsupported() {
        return new UnsupportedOperationException(
                "LCS is not supported on the Lettuce backend with Lettuce 6.5.x.");
    }
}
