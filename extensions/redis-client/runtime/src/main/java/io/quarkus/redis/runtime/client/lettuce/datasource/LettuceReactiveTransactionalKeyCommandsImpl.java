package io.quarkus.redis.runtime.client.lettuce.datasource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import io.quarkus.redis.datasource.keys.CopyArgs;
import io.quarkus.redis.datasource.keys.ExpireArgs;
import io.quarkus.redis.datasource.keys.ReactiveTransactionalKeyCommands;
import io.quarkus.redis.datasource.keys.RedisKeyNotFoundException;
import io.quarkus.redis.datasource.keys.RedisValueType;
import io.quarkus.redis.datasource.transactions.ReactiveTransactionalRedisDataSource;
import io.quarkus.redis.runtime.client.lettuce.key.LettuceReactiveKeyCommandsImpl;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveTransactionalKeyCommands}.
 * <p>
 * A thin transactional shell over {@link LettuceReactiveKeyCommandsImpl}: each command reuses the
 * non-transactional command-builder seam ({@code reactive._xxx(...)}) for validation and argument
 * conversion, then registers the {@link io.lettuce.core.RedisFuture} together with a result mapper on
 * the {@link LettuceTransactionHolder}. The mapper mirrors the {@code .map(...)} of the corresponding
 * non-transactional command, so the assembled {@code TransactionResult.get(index)} yields the same
 * Java type as the Vert.x backend.
 *
 * @param <K> the key type
 * @param <V> the value type used by the underlying connection
 */
public class LettuceReactiveTransactionalKeyCommandsImpl<K, V>
        implements ReactiveTransactionalKeyCommands<K> {

    private final ReactiveTransactionalRedisDataSource dataSource;
    private final LettuceReactiveKeyCommandsImpl<K, V> reactive;
    private final LettuceTransactionHolder tx;

    public LettuceReactiveTransactionalKeyCommandsImpl(ReactiveTransactionalRedisDataSource dataSource,
            LettuceReactiveKeyCommandsImpl<K, V> reactive, LettuceTransactionHolder tx) {
        this.dataSource = dataSource;
        this.reactive = reactive;
        this.tx = tx;
    }

    @Override
    public ReactiveTransactionalRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Void> copy(K source, K destination) {
        return tx.enqueue(reactive._copy(source, destination), v -> v);
    }

    @Override
    public Uni<Void> copy(K source, K destination, CopyArgs copyArgs) {
        return tx.enqueue(reactive._copy(source, destination, copyArgs), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> del(K... keys) {
        return tx.enqueue(reactive._del(keys), v -> v == null ? null : v.intValue());
    }

    @Override
    public Uni<Void> dump(K key) {
        return tx.enqueue(reactive._dump(key),
                bytes -> bytes == null ? null : new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public Uni<Void> exists(K key) {
        return tx.enqueue(reactive._exists(key), c -> c != null && c > 0);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> exists(K... keys) {
        return tx.enqueue(reactive._exists(keys), v -> v == null ? null : v.intValue());
    }

    @Override
    public Uni<Void> expire(K key, long seconds, ExpireArgs expireArgs) {
        return tx.enqueue(reactive._expire(key, seconds, expireArgs), v -> v);
    }

    @Override
    public Uni<Void> expire(K key, Duration duration, ExpireArgs expireArgs) {
        return tx.enqueue(reactive._expire(key, duration, expireArgs), v -> v);
    }

    @Override
    public Uni<Void> expire(K key, long seconds) {
        return tx.enqueue(reactive._expire(key, seconds), v -> v);
    }

    @Override
    public Uni<Void> expire(K key, Duration duration) {
        return tx.enqueue(reactive._expire(key, duration), v -> v);
    }

    @Override
    public Uni<Void> expireat(K key, long timestamp) {
        return tx.enqueue(reactive._expireat(key, timestamp), v -> v);
    }

    @Override
    public Uni<Void> expireat(K key, Instant timestamp) {
        return tx.enqueue(reactive._expireat(key, timestamp), v -> v);
    }

    @Override
    public Uni<Void> expireat(K key, long timestamp, ExpireArgs expireArgs) {
        return tx.enqueue(reactive._expireat(key, timestamp, expireArgs), v -> v);
    }

    @Override
    public Uni<Void> expireat(K key, Instant timestamp, ExpireArgs expireArgs) {
        return tx.enqueue(reactive._expireat(key, timestamp, expireArgs), v -> v);
    }

    @Override
    public Uni<Void> expiretime(K key) {
        return tx.enqueue(reactive._expiretime(key), r -> decodeExpireResponse(key, r));
    }

    @Override
    public Uni<Void> keys(String pattern) {
        return tx.enqueue(reactive._keys(pattern), v -> v);
    }

    @Override
    public Uni<Void> move(K key, long db) {
        return tx.enqueue(reactive._move(key, db), v -> v);
    }

    @Override
    public Uni<Void> persist(K key) {
        return tx.enqueue(reactive._persist(key), v -> v);
    }

    @Override
    public Uni<Void> pexpire(K key, long milliseconds, ExpireArgs expireArgs) {
        return tx.enqueue(reactive._pexpire(key, milliseconds, expireArgs), v -> v);
    }

    @Override
    public Uni<Void> pexpire(K key, Duration duration, ExpireArgs expireArgs) {
        return tx.enqueue(reactive._pexpire(key, duration, expireArgs), v -> v);
    }

    @Override
    public Uni<Void> pexpire(K key, long ms) {
        return tx.enqueue(reactive._pexpire(key, ms), v -> v);
    }

    @Override
    public Uni<Void> pexpire(K key, Duration duration) {
        return tx.enqueue(reactive._pexpire(key, duration), v -> v);
    }

    @Override
    public Uni<Void> pexpireat(K key, long timestamp) {
        return tx.enqueue(reactive._pexpireat(key, timestamp), v -> v);
    }

    @Override
    public Uni<Void> pexpireat(K key, Instant timestamp) {
        return tx.enqueue(reactive._pexpireat(key, timestamp), v -> v);
    }

    @Override
    public Uni<Void> pexpireat(K key, long timestamp, ExpireArgs expireArgs) {
        return tx.enqueue(reactive._pexpireat(key, timestamp, expireArgs), v -> v);
    }

    @Override
    public Uni<Void> pexpireat(K key, Instant timestamp, ExpireArgs expireArgs) {
        return tx.enqueue(reactive._pexpireat(key, timestamp, expireArgs), v -> v);
    }

    @Override
    public Uni<Void> pexpiretime(K key) {
        return tx.enqueue(reactive._pexpiretime(key), r -> decodeExpireResponse(key, r));
    }

    @Override
    public Uni<Void> pttl(K key) {
        return tx.enqueue(reactive._pttl(key), r -> decodeExpireResponse(key, r));
    }

    @Override
    public Uni<Void> randomkey() {
        return tx.enqueue(reactive._randomkey(), v -> v);
    }

    @Override
    public Uni<Void> rename(K key, K newkey) {
        return tx.enqueue(reactive._rename(key, newkey), v -> null);
    }

    @Override
    public Uni<Void> renamenx(K key, K newkey) {
        return tx.enqueue(reactive._renamenx(key, newkey), v -> v);
    }

    @SafeVarargs
    @Override
    public final Uni<Void> touch(K... keys) {
        return tx.enqueue(reactive._touch(keys), v -> v == null ? null : v.intValue());
    }

    @Override
    public Uni<Void> ttl(K key) throws RedisKeyNotFoundException {
        return tx.enqueue(reactive._ttl(key), r -> decodeExpireResponse(key, r));
    }

    @Override
    public Uni<Void> type(K key) {
        return tx.enqueue(reactive._type(key),
                s -> s == null ? null : RedisValueType.valueOf(s.toUpperCase()));
    }

    @SafeVarargs
    @Override
    public final Uni<Void> unlink(K... keys) {
        return tx.enqueue(reactive._unlink(keys), v -> v == null ? null : v.intValue());
    }

    private long decodeExpireResponse(K key, Long r) {
        if (r != null && r == -2L) {
            throw new RedisKeyNotFoundException(String.valueOf(key));
        }
        return r;
    }
}
