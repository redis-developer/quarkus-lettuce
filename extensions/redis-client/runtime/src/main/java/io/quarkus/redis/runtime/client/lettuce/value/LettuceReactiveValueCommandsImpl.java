package io.quarkus.redis.runtime.client.lettuce.value;

import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;
import static io.smallrye.mutiny.helpers.ParameterValidation.positiveOrZero;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveValueCommands}.
 * <p>
 * Delegates every command to Lettuce async APIs and adapts the resulting
 * {@link java.util.concurrent.CompletionStage} to {@link Uni} via {@link LettuceResult#toUni}.
 * <p>
 * <strong>Note on {@code LCS}</strong>: not yet implemented. Lettuce 7 exposes native
 * {@code lcs(LcsArgs)} support, so {@link #lcs} and {@link #lcsLength} can now be wired
 * up; until then they throw {@link UnsupportedOperationException}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LettuceReactiveValueCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveValueCommands<K, V> {

    private final ReactiveRedisDataSource dataSource;

    public LettuceReactiveValueCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection) {
        super(connection);
        LettuceValueCommandsConverters.register();
        this.dataSource = dataSource;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Long> append(K key, V value) {
        return LettuceResult.toUni(_append(key, value));
    }

    public Supplier<RedisFuture<Long>> _append(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.append(key, value);
    }

    @Override
    public Uni<Long> decr(K key) {
        return LettuceResult.toUni(_decr(key));
    }

    public Supplier<RedisFuture<Long>> _decr(K key) {
        nonNull(key, "key");
        return () -> async.decr(key);
    }

    @Override
    public Uni<Long> decrby(K key, long amount) {
        return LettuceResult.toUni(_decrby(key, amount));
    }

    public Supplier<RedisFuture<Long>> _decrby(K key, long amount) {
        nonNull(key, "key");
        return () -> async.decrby(key, amount);
    }

    @Override
    public Uni<V> get(K key) {
        return LettuceResult.toUni(_get(key));
    }

    public Supplier<RedisFuture<V>> _get(K key) {
        nonNull(key, "key");
        return () -> async.get(key);
    }

    @Override
    public Uni<V> getdel(K key) {
        return LettuceResult.toUni(_getdel(key));
    }

    public Supplier<RedisFuture<V>> _getdel(K key) {
        nonNull(key, "key");
        return () -> async.getdel(key);
    }

    @Override
    public Uni<V> getex(K key, GetExArgs args) {
        return LettuceResult.toUni(_getex(key, args));
    }

    public Supplier<RedisFuture<V>> _getex(K key, GetExArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        io.lettuce.core.GetExArgs lettuceArgs = LettuceConverterRegistry.convertArg(args);
        return () -> async.getex(key, lettuceArgs);
    }

    @Override
    public Uni<String> getrange(K key, long start, long end) {
        return LettuceResult.toUni(_getrange(key, start, end))
                .map(v -> v == null ? null : v.toString());
    }

    public Supplier<RedisFuture<V>> _getrange(K key, long start, long end) {
        nonNull(key, "key");
        positiveOrZero(start, "start");
        return () -> async.getrange(key, start, end);
    }

    @Override
    public Uni<V> getset(K key, V value) {
        return LettuceResult.toUni(_getset(key, value));
    }

    public Supplier<RedisFuture<V>> _getset(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.getset(key, value);
    }

    @Override
    public Uni<Long> incr(K key) {
        return LettuceResult.toUni(_incr(key));
    }

    public Supplier<RedisFuture<Long>> _incr(K key) {
        nonNull(key, "key");
        return () -> async.incr(key);
    }

    @Override
    public Uni<Long> incrby(K key, long amount) {
        return LettuceResult.toUni(_incrby(key, amount));
    }

    public Supplier<RedisFuture<Long>> _incrby(K key, long amount) {
        nonNull(key, "key");
        return () -> async.incrby(key, amount);
    }

    @Override
    public Uni<Double> incrbyfloat(K key, double amount) {
        return LettuceResult.toUni(_incrbyfloat(key, amount));
    }

    public Supplier<RedisFuture<Double>> _incrbyfloat(K key, double amount) {
        nonNull(key, "key");
        return () -> async.incrbyfloat(key, amount);
    }

    @Override
    public Uni<String> lcs(K key1, K key2) {
        return Uni.createFrom().failure(lcsUnsupported());
    }

    @Override
    public Uni<Long> lcsLength(K key1, K key2) {
        return Uni.createFrom().failure(lcsUnsupported());
    }

    private static UnsupportedOperationException lcsUnsupported() {
        return new UnsupportedOperationException(
                "LCS is not yet implemented on the Lettuce backend. "
                        + "Set quarkus.redis.backend=vertx if you need LCS.");
    }

    @SafeVarargs
    @Override
    public final Uni<Map<K, V>> mget(K... keys) {
        return LettuceResult.toUni(_mget(keys)).map(this::toOrderedMap);
    }

    @SafeVarargs
    public final Supplier<RedisFuture<List<KeyValue<K, V>>>> _mget(K... keys) {
        nonNull(keys, "keys");
        if (keys.length == 0) {
            throw new IllegalArgumentException("`keys` must not be empty");
        }
        doesNotContainNull(keys, "keys");
        return () -> async.mget(keys);
    }

    public Map<K, V> toOrderedMap(List<KeyValue<K, V>> results) {
        Map<K, V> map = new LinkedHashMap<>();
        for (KeyValue<K, V> kv : results) {
            map.put(kv.getKey(), kv.hasValue() ? kv.getValue() : null);
        }
        return map;
    }

    @Override
    public Uni<Void> mset(Map<K, V> map) {
        return LettuceResult.toUni(_mset(map)).replaceWithVoid();
    }

    public Supplier<RedisFuture<String>> _mset(Map<K, V> map) {
        requireNonEmpty(map);
        return () -> async.mset(map);
    }

    @Override
    public Uni<Boolean> msetnx(Map<K, V> map) {
        return LettuceResult.toUni(_msetnx(map));
    }

    public Supplier<RedisFuture<Boolean>> _msetnx(Map<K, V> map) {
        requireNonEmpty(map);
        return () -> async.msetnx(map);
    }

    @Override
    public Uni<Void> psetex(K key, long milliseconds, V value) {
        return LettuceResult.toUni(_psetex(key, milliseconds, value)).replaceWithVoid();
    }

    public Supplier<RedisFuture<String>> _psetex(K key, long milliseconds, V value) {
        nonNull(key, "key");
        positive(milliseconds, "milliseconds");
        nonNull(value, "value");
        return () -> async.psetex(key, milliseconds, value);
    }

    @Override
    public Uni<Void> set(K key, V value) {
        return LettuceResult.toUni(_set(key, value)).replaceWithVoid();
    }

    public Supplier<RedisFuture<String>> _set(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.set(key, value);
    }

    @Override
    public Uni<Void> set(K key, V value, SetArgs setArgs) {
        return LettuceResult.toUni(_set(key, value, setArgs)).replaceWithVoid();
    }

    public Supplier<RedisFuture<String>> _set(K key, V value, SetArgs setArgs) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(setArgs, "setArgs");
        io.lettuce.core.SetArgs lettuceArgs = LettuceConverterRegistry.convertArg(setArgs);
        return () -> async.set(key, value, lettuceArgs);
    }

    @Override
    public Uni<Boolean> setAndChanged(K key, V value) {
        return LettuceResult.toUni(_set(key, value)).map(LettuceReactiveValueCommandsImpl::isOk);
    }

    @Override
    public Uni<Boolean> setAndChanged(K key, V value, SetArgs setArgs) {
        return LettuceResult.toUni(_set(key, value, setArgs))
                .map(LettuceReactiveValueCommandsImpl::isOk);
    }

    @Override
    public Uni<V> setGet(K key, V value) {
        return LettuceResult.toUni(_setGet(key, value));
    }

    public Supplier<RedisFuture<V>> _setGet(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.setGet(key, value);
    }

    @Override
    public Uni<V> setGet(K key, V value, SetArgs setArgs) {
        return LettuceResult.toUni(_setGet(key, value, setArgs));
    }

    public Supplier<RedisFuture<V>> _setGet(K key, V value, SetArgs setArgs) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(setArgs, "setArgs");
        io.lettuce.core.SetArgs lettuceArgs = LettuceConverterRegistry.convertArg(setArgs);
        return () -> async.setGet(key, value, lettuceArgs);
    }

    @Override
    public Uni<Void> setex(K key, long seconds, V value) {
        return LettuceResult.toUni(_setex(key, seconds, value)).replaceWithVoid();
    }

    public Supplier<RedisFuture<String>> _setex(K key, long seconds, V value) {
        nonNull(key, "key");
        positive(seconds, "seconds");
        nonNull(value, "value");
        return () -> async.setex(key, seconds, value);
    }

    @Override
    public Uni<Boolean> setnx(K key, V value) {
        return LettuceResult.toUni(_setnx(key, value));
    }

    public Supplier<RedisFuture<Boolean>> _setnx(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.setnx(key, value);
    }

    @Override
    public Uni<Long> setrange(K key, long offset, V value) {
        return LettuceResult.toUni(_setrange(key, offset, value));
    }

    public Supplier<RedisFuture<Long>> _setrange(K key, long offset, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        positiveOrZero(offset, "offset");
        return () -> async.setrange(key, offset, value);
    }

    @Override
    public Uni<Long> strlen(K key) {
        return LettuceResult.toUni(_strlen(key));
    }

    public Supplier<RedisFuture<Long>> _strlen(K key) {
        nonNull(key, "key");
        return () -> async.strlen(key);
    }

    public static boolean isOk(String response) {
        return "OK".equals(response);
    }

    private static <K, V> void requireNonEmpty(Map<K, V> map) {
        if (map == null || map.isEmpty()) {
            throw new IllegalArgumentException("`map` must not be null or empty");
        }
    }
}
