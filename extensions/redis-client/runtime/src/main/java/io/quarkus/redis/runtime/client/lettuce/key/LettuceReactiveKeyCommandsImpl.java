package io.quarkus.redis.runtime.client.lettuce.key;

import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positiveOrZero;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

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

    static {
        LettuceKeyCommandsConverters.register();
    }

    public LettuceReactiveKeyCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection) {
        super(connection);
        this.dataSource = dataSource;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Boolean> copy(K source, K destination) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        return LettuceResult.toUni(() -> async.copy(source, destination));
    }

    @Override
    public Uni<Boolean> copy(K source, K destination, CopyArgs copyArgs) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        nonNull(copyArgs, "copyArgs");
        io.lettuce.core.CopyArgs lettuceArgs = LettuceConverterRegistry.convertArg(copyArgs);
        return LettuceResult.toUni(() -> async.copy(source, destination, lettuceArgs));
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> del(K... keys) {
        notEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        return LettuceResult.toUni(() -> async.del(keys)).map(Long::intValue);
    }

    @Override
    public Uni<String> dump(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.dump(key))
                .map(bytes -> bytes == null ? null : new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Uni<Boolean> exists(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.exists(key)).map(c -> c != null && c > 0);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> exists(K... keys) {
        notEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        return LettuceResult.toUni(() -> async.exists(keys)).map(Long::intValue);
    }

    @Override
    public Uni<Boolean> expire(K key, long seconds, ExpireArgs expireArgs) {
        nonNull(key, "key");
        nonNull(expireArgs, "expireArgs");
        io.lettuce.core.ExpireArgs lettuceArgs = LettuceConverterRegistry.convertArg(expireArgs);
        return LettuceResult.toUni(() -> async.expire(key, seconds, lettuceArgs));
    }

    @Override
    public Uni<Boolean> expire(K key, Duration duration, ExpireArgs expireArgs) {
        nonNull(duration, "duration");
        return expire(key, duration.toSeconds(), expireArgs);
    }

    @Override
    public Uni<Boolean> expire(K key, long seconds) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.expire(key, seconds));
    }

    @Override
    public Uni<Boolean> expire(K key, Duration duration) {
        nonNull(duration, "duration");
        return expire(key, duration.toSeconds());
    }

    @Override
    public Uni<Boolean> expireat(K key, long timestamp) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.expireat(key, timestamp));
    }

    @Override
    public Uni<Boolean> expireat(K key, Instant timestamp) {
        nonNull(timestamp, "timestamp");
        return expireat(key, timestamp.getEpochSecond());
    }

    @Override
    public Uni<Boolean> expireat(K key, long timestamp, ExpireArgs expireArgs) {
        nonNull(key, "key");
        nonNull(expireArgs, "expireArgs");
        io.lettuce.core.ExpireArgs lettuceArgs = LettuceConverterRegistry.convertArg(expireArgs);
        return LettuceResult.toUni(() -> async.expireat(key, timestamp, lettuceArgs));
    }

    @Override
    public Uni<Boolean> expireat(K key, Instant timestamp, ExpireArgs expireArgs) {
        nonNull(timestamp, "timestamp");
        return expireat(key, timestamp.getEpochSecond(), expireArgs);
    }

    @Override
    public Uni<Long> expiretime(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.expiretime(key)).map(r -> decodeExpireResponse(key, r));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Uni<List<K>> keys(String pattern) {
        nonNull(pattern, "pattern");
        if (pattern.isBlank()) {
            throw new IllegalArgumentException("`pattern` must not be blank");
        }
        // Lettuce's keys(K) accepts a K-typed pattern; the connection codec encodes it. For
        // typical String-keyed connections this is a no-op cast.
        return LettuceResult.toUni(() -> async.keys((K) pattern));
    }

    @Override
    public Uni<Boolean> move(K key, long db) {
        nonNull(key, "key");
        positiveOrZero(db, "db");
        if (db > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("`db` must fit in a positive int");
        }
        return LettuceResult.toUni(() -> async.move(key, (int) db));
    }

    @Override
    public Uni<Boolean> persist(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.persist(key));
    }

    @Override
    public Uni<Boolean> pexpire(K key, long milliseconds, ExpireArgs expireArgs) {
        nonNull(key, "key");
        nonNull(expireArgs, "expireArgs");
        io.lettuce.core.ExpireArgs lettuceArgs = LettuceConverterRegistry.convertArg(expireArgs);
        return LettuceResult.toUni(() -> async.pexpire(key, milliseconds, lettuceArgs));
    }

    @Override
    public Uni<Boolean> pexpire(K key, Duration duration, ExpireArgs expireArgs) {
        nonNull(duration, "duration");
        return pexpire(key, duration.toMillis(), expireArgs);
    }

    @Override
    public Uni<Boolean> pexpire(K key, long ms) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.pexpire(key, ms));
    }

    @Override
    public Uni<Boolean> pexpire(K key, Duration duration) {
        nonNull(duration, "duration");
        return pexpire(key, duration.toMillis());
    }

    @Override
    public Uni<Boolean> pexpireat(K key, long timestamp) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.pexpireat(key, timestamp));
    }

    @Override
    public Uni<Boolean> pexpireat(K key, Instant timestamp) {
        nonNull(timestamp, "timestamp");
        return pexpireat(key, timestamp.toEpochMilli());
    }

    @Override
    public Uni<Boolean> pexpireat(K key, long timestamp, ExpireArgs expireArgs) {
        nonNull(key, "key");
        nonNull(expireArgs, "expireArgs");
        io.lettuce.core.ExpireArgs lettuceArgs = LettuceConverterRegistry.convertArg(expireArgs);
        return LettuceResult.toUni(() -> async.pexpireat(key, timestamp, lettuceArgs));
    }

    @Override
    public Uni<Boolean> pexpireat(K key, Instant timestamp, ExpireArgs expireArgs) {
        nonNull(timestamp, "timestamp");
        return pexpireat(key, timestamp.toEpochMilli(), expireArgs);
    }

    @Override
    public Uni<Long> pexpiretime(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.pexpiretime(key)).map(r -> decodeExpireResponse(key, r));
    }

    @Override
    public Uni<Long> pttl(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.pttl(key)).map(r -> decodeExpireResponse(key, r));
    }

    @Override
    public Uni<K> randomkey() {
        return LettuceResult.toUni(async::randomkey);
    }

    @Override
    public Uni<Void> rename(K key, K newkey) {
        nonNull(key, "key");
        nonNull(newkey, "newkey");
        return LettuceResult.<String> toUni(() -> async.rename(key, newkey))
                .onFailure().transform(t -> mapNoSuchKey(key, t))
                .replaceWithVoid();
    }

    @Override
    public Uni<Boolean> renamenx(K key, K newkey) {
        nonNull(key, "key");
        nonNull(newkey, "newkey");
        return LettuceResult.<Boolean> toUni(() -> async.renamenx(key, newkey))
                .onFailure().transform(t -> mapNoSuchKey(key, t));
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
        notEmpty(keys, "keys");
        return LettuceResult.toUni(() -> async.touch(keys)).map(Long::intValue);
    }

    @Override
    public Uni<Long> ttl(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.ttl(key)).map(r -> decodeExpireResponse(key, r));
    }

    @Override
    public Uni<RedisValueType> type(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.type(key))
                .map(s -> s == null ? null : RedisValueType.valueOf(s.toUpperCase()));
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> unlink(K... keys) {
        notEmpty(keys, "keys");
        return LettuceResult.toUni(() -> async.unlink(keys)).map(Long::intValue);
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
