package io.quarkus.redis.runtime.client.lettuce.key;

import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positiveOrZero;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.keys.CopyArgs;
import io.quarkus.redis.datasource.keys.ExpireArgs;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.keys.ReactiveKeyScanCursor;
import io.quarkus.redis.datasource.keys.RedisKeyNotFoundException;
import io.quarkus.redis.datasource.keys.RedisValueType;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveKeyCommands}.
 * <p>
 * Delegates every command to Lettuce async APIs and adapts the resulting
 * {@link java.util.concurrent.CompletionStage} to {@link Uni} via {@link LettuceResult#toUni}.
 *
 * @param <K> the key type
 * @param <V> the value type used by the underlying connection
 */
public class LettuceReactiveKeyCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveKeyCommands<K> {

    private final ReactiveRedisDataSource dataSource;

    public LettuceReactiveKeyCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection) {
        super(connection);
        LettuceKeyCommandsConverters.register();
        this.dataSource = dataSource;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Boolean> copy(K source, K destination) {
        return LettuceResult.toUni(_copy(source, destination));
    }

    public Supplier<RedisFuture<Boolean>> _copy(K source, K destination) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        return () -> async.copy(source, destination);
    }

    @Override
    public Uni<Boolean> copy(K source, K destination, CopyArgs copyArgs) {
        return LettuceResult.toUni(_copy(source, destination, copyArgs));
    }

    public Supplier<RedisFuture<Boolean>> _copy(K source, K destination, CopyArgs copyArgs) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        nonNull(copyArgs, "copyArgs");
        io.lettuce.core.CopyArgs lettuceArgs = LettuceConverterRegistry.convertArg(copyArgs);
        return () -> async.copy(source, destination, lettuceArgs);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> del(K... keys) {
        return LettuceResult.toUni(_del(keys)).map(Long::intValue);
    }

    @SafeVarargs
    public final Supplier<RedisFuture<Long>> _del(K... keys) {
        notEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        return () -> async.del(keys);
    }

    @Override
    public Uni<String> dump(K key) {
        return LettuceResult.toUni(_dump(key))
                .map(bytes -> bytes == null ? null : new String(bytes, StandardCharsets.UTF_8));
    }

    public Supplier<RedisFuture<byte[]>> _dump(K key) {
        nonNull(key, "key");
        return () -> async.dump(key);
    }

    @Override
    public Uni<Boolean> exists(K key) {
        return LettuceResult.toUni(_exists(key)).map(c -> c != null && c > 0);
    }

    @SuppressWarnings("unchecked")
    public Supplier<RedisFuture<Long>> _exists(K key) {
        nonNull(key, "key");
        return () -> async.exists(key);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> exists(K... keys) {
        return LettuceResult.toUni(_exists(keys)).map(Long::intValue);
    }

    @SafeVarargs
    public final Supplier<RedisFuture<Long>> _exists(K... keys) {
        notEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        return () -> async.exists(keys);
    }

    @Override
    public Uni<Boolean> expire(K key, long seconds, ExpireArgs expireArgs) {
        return LettuceResult.toUni(_expire(key, seconds, expireArgs));
    }

    public Supplier<RedisFuture<Boolean>> _expire(K key, long seconds, ExpireArgs expireArgs) {
        nonNull(key, "key");
        nonNull(expireArgs, "expireArgs");
        io.lettuce.core.ExpireArgs lettuceArgs = LettuceConverterRegistry.convertArg(expireArgs);
        return () -> async.expire(key, seconds, lettuceArgs);
    }

    @Override
    public Uni<Boolean> expire(K key, Duration duration, ExpireArgs expireArgs) {
        return LettuceResult.toUni(_expire(key, duration, expireArgs));
    }

    public Supplier<RedisFuture<Boolean>> _expire(K key, Duration duration, ExpireArgs expireArgs) {
        nonNull(duration, "duration");
        return _expire(key, duration.toSeconds(), expireArgs);
    }

    @Override
    public Uni<Boolean> expire(K key, long seconds) {
        return LettuceResult.toUni(_expire(key, seconds));
    }

    public Supplier<RedisFuture<Boolean>> _expire(K key, long seconds) {
        nonNull(key, "key");
        return () -> async.expire(key, seconds);
    }

    @Override
    public Uni<Boolean> expire(K key, Duration duration) {
        return LettuceResult.toUni(_expire(key, duration));
    }

    public Supplier<RedisFuture<Boolean>> _expire(K key, Duration duration) {
        nonNull(duration, "duration");
        return _expire(key, duration.toSeconds());
    }

    @Override
    public Uni<Boolean> expireat(K key, long timestamp) {
        return LettuceResult.toUni(_expireat(key, timestamp));
    }

    public Supplier<RedisFuture<Boolean>> _expireat(K key, long timestamp) {
        nonNull(key, "key");
        return () -> async.expireat(key, timestamp);
    }

    @Override
    public Uni<Boolean> expireat(K key, Instant timestamp) {
        return LettuceResult.toUni(_expireat(key, timestamp));
    }

    public Supplier<RedisFuture<Boolean>> _expireat(K key, Instant timestamp) {
        nonNull(timestamp, "timestamp");
        return _expireat(key, timestamp.getEpochSecond());
    }

    @Override
    public Uni<Boolean> expireat(K key, long timestamp, ExpireArgs expireArgs) {
        return LettuceResult.toUni(_expireat(key, timestamp, expireArgs));
    }

    public Supplier<RedisFuture<Boolean>> _expireat(K key, long timestamp, ExpireArgs expireArgs) {
        nonNull(key, "key");
        nonNull(expireArgs, "expireArgs");
        io.lettuce.core.ExpireArgs lettuceArgs = LettuceConverterRegistry.convertArg(expireArgs);
        return () -> async.expireat(key, timestamp, lettuceArgs);
    }

    @Override
    public Uni<Boolean> expireat(K key, Instant timestamp, ExpireArgs expireArgs) {
        return LettuceResult.toUni(_expireat(key, timestamp, expireArgs));
    }

    public Supplier<RedisFuture<Boolean>> _expireat(K key, Instant timestamp, ExpireArgs expireArgs) {
        nonNull(timestamp, "timestamp");
        return _expireat(key, timestamp.getEpochSecond(), expireArgs);
    }

    @Override
    public Uni<Long> expiretime(K key) {
        return LettuceResult.toUni(_expiretime(key)).map(r -> decodeExpireResponse(key, r));
    }

    public Supplier<RedisFuture<Long>> _expiretime(K key) {
        nonNull(key, "key");
        return () -> async.expiretime(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Uni<List<K>> keys(String pattern) {
        nonNull(pattern, "pattern");
        if (pattern.isBlank()) {
            throw new IllegalArgumentException("`pattern` must not be blank");
        }
        return LettuceResult.toUni(_keys(pattern));
    }

    @SuppressWarnings("unchecked")
    public Supplier<RedisFuture<List<K>>> _keys(String pattern) {
        nonNull(pattern, "pattern");
        if (pattern.isBlank()) {
            throw new IllegalArgumentException("`pattern` must not be blank");
        }
        // Lettuce's keys(K) accepts a K-typed pattern; the connection codec encodes it. For
        // typical String-keyed connections this is a no-op cast.
        return () -> async.keys((K) pattern);
    }

    @Override
    public Uni<Boolean> move(K key, long db) {
        return LettuceResult.toUni(_move(key, db));
    }

    public Supplier<RedisFuture<Boolean>> _move(K key, long db) {
        nonNull(key, "key");
        positiveOrZero(db, "db");
        if (db > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("`db` must fit in a positive int");
        }
        return () -> async.move(key, (int) db);
    }

    @Override
    public Uni<Boolean> persist(K key) {
        return LettuceResult.toUni(_persist(key));
    }

    public Supplier<RedisFuture<Boolean>> _persist(K key) {
        nonNull(key, "key");
        return () -> async.persist(key);
    }

    @Override
    public Uni<Boolean> pexpire(K key, long milliseconds, ExpireArgs expireArgs) {
        return LettuceResult.toUni(_pexpire(key, milliseconds, expireArgs));
    }

    public Supplier<RedisFuture<Boolean>> _pexpire(K key, long milliseconds, ExpireArgs expireArgs) {
        nonNull(key, "key");
        nonNull(expireArgs, "expireArgs");
        io.lettuce.core.ExpireArgs lettuceArgs = LettuceConverterRegistry.convertArg(expireArgs);
        return () -> async.pexpire(key, milliseconds, lettuceArgs);
    }

    @Override
    public Uni<Boolean> pexpire(K key, Duration duration, ExpireArgs expireArgs) {
        return LettuceResult.toUni(_pexpire(key, duration, expireArgs));
    }

    public Supplier<RedisFuture<Boolean>> _pexpire(K key, Duration duration, ExpireArgs expireArgs) {
        nonNull(duration, "duration");
        return _pexpire(key, duration.toMillis(), expireArgs);
    }

    @Override
    public Uni<Boolean> pexpire(K key, long ms) {
        return LettuceResult.toUni(_pexpire(key, ms));
    }

    public Supplier<RedisFuture<Boolean>> _pexpire(K key, long ms) {
        nonNull(key, "key");
        return () -> async.pexpire(key, ms);
    }

    @Override
    public Uni<Boolean> pexpire(K key, Duration duration) {
        return LettuceResult.toUni(_pexpire(key, duration));
    }

    public Supplier<RedisFuture<Boolean>> _pexpire(K key, Duration duration) {
        nonNull(duration, "duration");
        return _pexpire(key, duration.toMillis());
    }

    @Override
    public Uni<Boolean> pexpireat(K key, long timestamp) {
        return LettuceResult.toUni(_pexpireat(key, timestamp));
    }

    public Supplier<RedisFuture<Boolean>> _pexpireat(K key, long timestamp) {
        nonNull(key, "key");
        return () -> async.pexpireat(key, timestamp);
    }

    @Override
    public Uni<Boolean> pexpireat(K key, Instant timestamp) {
        return LettuceResult.toUni(_pexpireat(key, timestamp));
    }

    public Supplier<RedisFuture<Boolean>> _pexpireat(K key, Instant timestamp) {
        nonNull(timestamp, "timestamp");
        return _pexpireat(key, timestamp.toEpochMilli());
    }

    @Override
    public Uni<Boolean> pexpireat(K key, long timestamp, ExpireArgs expireArgs) {
        return LettuceResult.toUni(_pexpireat(key, timestamp, expireArgs));
    }

    public Supplier<RedisFuture<Boolean>> _pexpireat(K key, long timestamp, ExpireArgs expireArgs) {
        nonNull(key, "key");
        nonNull(expireArgs, "expireArgs");
        io.lettuce.core.ExpireArgs lettuceArgs = LettuceConverterRegistry.convertArg(expireArgs);
        return () -> async.pexpireat(key, timestamp, lettuceArgs);
    }

    @Override
    public Uni<Boolean> pexpireat(K key, Instant timestamp, ExpireArgs expireArgs) {
        return LettuceResult.toUni(_pexpireat(key, timestamp, expireArgs));
    }

    public Supplier<RedisFuture<Boolean>> _pexpireat(K key, Instant timestamp, ExpireArgs expireArgs) {
        nonNull(timestamp, "timestamp");
        return _pexpireat(key, timestamp.toEpochMilli(), expireArgs);
    }

    @Override
    public Uni<Long> pexpiretime(K key) {
        return LettuceResult.toUni(_pexpiretime(key)).map(r -> decodeExpireResponse(key, r));
    }

    public Supplier<RedisFuture<Long>> _pexpiretime(K key) {
        nonNull(key, "key");
        return () -> async.pexpiretime(key);
    }

    @Override
    public Uni<Long> pttl(K key) {
        return LettuceResult.toUni(_pttl(key)).map(r -> decodeExpireResponse(key, r));
    }

    public Supplier<RedisFuture<Long>> _pttl(K key) {
        nonNull(key, "key");
        return () -> async.pttl(key);
    }

    @Override
    public Uni<K> randomkey() {
        return LettuceResult.toUni(_randomkey());
    }

    public Supplier<RedisFuture<K>> _randomkey() {
        return async::randomkey;
    }

    @Override
    public Uni<Void> rename(K key, K newkey) {
        return LettuceResult.<String> toUni(_rename(key, newkey))
                .onFailure().transform(t -> mapNoSuchKey(key, t))
                .replaceWithVoid();
    }

    public Supplier<RedisFuture<String>> _rename(K key, K newkey) {
        nonNull(key, "key");
        nonNull(newkey, "newkey");
        return () -> async.rename(key, newkey);
    }

    @Override
    public Uni<Boolean> renamenx(K key, K newkey) {
        return LettuceResult.<Boolean> toUni(_renamenx(key, newkey))
                .onFailure().transform(t -> mapNoSuchKey(key, t));
    }

    public Supplier<RedisFuture<Boolean>> _renamenx(K key, K newkey) {
        nonNull(key, "key");
        nonNull(newkey, "newkey");
        return () -> async.renamenx(key, newkey);
    }

    private Throwable mapNoSuchKey(K key, Throwable t) {
        String msg = t.getMessage();
        if (msg != null && msg.toLowerCase().contains("no such key")) {
            return new java.util.NoSuchElementException(String.valueOf(key));
        }
        return t;
    }

    @Override
    public ReactiveKeyScanCursor<K> scan() {
        return new LettuceKeyScanReactiveCursorImpl<>(async, null);
    }

    @Override
    public ReactiveKeyScanCursor<K> scan(KeyScanArgs args) {
        nonNull(args, "args");
        io.lettuce.core.KeyScanArgs lettuceArgs = LettuceConverterRegistry.convertArg(args);
        return new LettuceKeyScanReactiveCursorImpl<>(async, lettuceArgs);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> touch(K... keys) {
        return LettuceResult.toUni(_touch(keys)).map(Long::intValue);
    }

    @SafeVarargs
    public final Supplier<RedisFuture<Long>> _touch(K... keys) {
        notEmpty(keys, "keys");
        return () -> async.touch(keys);
    }

    @Override
    public Uni<Long> ttl(K key) {
        return LettuceResult.toUni(_ttl(key)).map(r -> decodeExpireResponse(key, r));
    }

    public Supplier<RedisFuture<Long>> _ttl(K key) {
        nonNull(key, "key");
        return () -> async.ttl(key);
    }

    @Override
    public Uni<RedisValueType> type(K key) {
        return LettuceResult.toUni(_type(key))
                .map(s -> s == null ? null : RedisValueType.valueOf(s.toUpperCase()));
    }

    public Supplier<RedisFuture<String>> _type(K key) {
        nonNull(key, "key");
        return () -> async.type(key);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> unlink(K... keys) {
        return LettuceResult.toUni(_unlink(keys)).map(Long::intValue);
    }

    @SafeVarargs
    public final Supplier<RedisFuture<Long>> _unlink(K... keys) {
        notEmpty(keys, "keys");
        return () -> async.unlink(keys);
    }

    private long decodeExpireResponse(K key, Long r) {
        if (r == -2L) {
            throw new RedisKeyNotFoundException(String.valueOf(key));
        }
        return r;
    }

    private static <T> void notEmpty(T[] array, String name) {
        nonNull(array, name);
        if (array.length == 0) {
            throw new IllegalArgumentException("`" + name + "` must not be empty");
        }
    }
}
