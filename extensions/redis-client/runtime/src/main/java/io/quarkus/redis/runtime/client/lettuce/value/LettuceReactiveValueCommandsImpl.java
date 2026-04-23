package io.quarkus.redis.runtime.client.lettuce.value;

import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;
import static io.smallrye.mutiny.helpers.ParameterValidation.positiveOrZero;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.lettuce.core.KeyValue;
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
 * <strong>Note on {@code LCS}</strong>: Lettuce 6.5.x only exposes the deprecated
 * {@code STRALGO LCS} command, which was removed in Redis 7.0. {@link #lcs} and
 * {@link #lcsLength} therefore throw {@link UnsupportedOperationException} on this
 * Lettuce version. Native {@code lcs(LcsArgs)} support will be wired in with the
 * Quarkus 4 / Lettuce 7 upgrade.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LettuceReactiveValueCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveValueCommands<K, V> {

    private final ReactiveRedisDataSource dataSource;

    static {
        LettuceValueCommandsConverters.register();
    }

    public LettuceReactiveValueCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection) {
        super(connection);
        this.dataSource = dataSource;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Long> append(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> async.append(key, value));
    }

    @Override
    public Uni<Long> decr(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.decr(key));
    }

    @Override
    public Uni<Long> decrby(K key, long amount) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.decrby(key, amount));
    }

    @Override
    public Uni<V> get(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.get(key));
    }

    @Override
    public Uni<V> getdel(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.getdel(key));
    }

    @Override
    public Uni<V> getex(K key, GetExArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        io.lettuce.core.GetExArgs lettuceArgs = LettuceConverterRegistry.convertArg(args);
        return LettuceResult.toUni(() -> async.getex(key, lettuceArgs));
    }

    @Override
    public Uni<String> getrange(K key, long start, long end) {
        nonNull(key, "key");
        positiveOrZero(start, "start");
        return LettuceResult.toUni(() -> async.getrange(key, start, end))
                .map(v -> v == null ? null : v.toString());
    }

    @Override
    @SuppressWarnings("deprecation")
    public Uni<V> getset(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> async.getset(key, value));
    }

    @Override
    public Uni<Long> incr(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.incr(key));
    }

    @Override
    public Uni<Long> incrby(K key, long amount) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.incrby(key, amount));
    }

    @Override
    public Uni<Double> incrbyfloat(K key, double amount) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.incrbyfloat(key, amount));
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
                "LCS is not supported on the Lettuce backend with Lettuce 6.5.x: the only available API "
                        + "emits the deprecated STRALGO command, which was removed in Redis 7.0. "
                        + "Native LCS support will be enabled with the Quarkus 4 / Lettuce 7 upgrade.");
    }

    @SafeVarargs
    @Override
    public final Uni<Map<K, V>> mget(K... keys) {
        nonNull(keys, "keys");
        if (keys.length == 0) {
            throw new IllegalArgumentException("`keys` must not be empty");
        }
        doesNotContainNull(keys, "keys");
        return LettuceResult.toUni(() -> async.mget(keys))
                .map(this::toOrderedMap);
    }

    private Map<K, V> toOrderedMap(List<KeyValue<K, V>> results) {
        Map<K, V> map = new LinkedHashMap<>();
        for (KeyValue<K, V> kv : results) {
            map.put(kv.getKey(), kv.hasValue() ? kv.getValue() : null);
        }
        return map;
    }

    @Override
    public Uni<Void> mset(Map<K, V> map) {
        requireNonEmpty(map, "map");
        return LettuceResult.toUni(() -> async.mset(map)).replaceWithVoid();
    }

    @Override
    public Uni<Boolean> msetnx(Map<K, V> map) {
        requireNonEmpty(map, "map");
        return LettuceResult.toUni(() -> async.msetnx(map));
    }

    @Override
    public Uni<Void> psetex(K key, long milliseconds, V value) {
        nonNull(key, "key");
        positive(milliseconds, "milliseconds");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> async.psetex(key, milliseconds, value)).replaceWithVoid();
    }

    @Override
    public Uni<Void> set(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> async.set(key, value)).replaceWithVoid();
    }

    @Override
    public Uni<Void> set(K key, V value, SetArgs setArgs) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(setArgs, "setArgs");
        io.lettuce.core.SetArgs lettuceArgs = LettuceConverterRegistry.convertArg(setArgs);
        return LettuceResult.toUni(() -> async.set(key, value, lettuceArgs)).replaceWithVoid();
    }

    @Override
    public Uni<Boolean> setAndChanged(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> async.set(key, value)).map(LettuceReactiveValueCommandsImpl::isOk);
    }

    @Override
    public Uni<Boolean> setAndChanged(K key, V value, SetArgs setArgs) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(setArgs, "setArgs");
        io.lettuce.core.SetArgs lettuceArgs = LettuceConverterRegistry.convertArg(setArgs);
        return LettuceResult.toUni(() -> async.set(key, value, lettuceArgs))
                .map(LettuceReactiveValueCommandsImpl::isOk);
    }

    @Override
    public Uni<V> setGet(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> async.setGet(key, value));
    }

    @Override
    public Uni<V> setGet(K key, V value, SetArgs setArgs) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(setArgs, "setArgs");
        io.lettuce.core.SetArgs lettuceArgs = LettuceConverterRegistry.convertArg(setArgs);
        return LettuceResult.toUni(() -> async.setGet(key, value, lettuceArgs));
    }

    @Override
    public Uni<Void> setex(K key, long seconds, V value) {
        nonNull(key, "key");
        positive(seconds, "seconds");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> async.setex(key, seconds, value)).replaceWithVoid();
    }

    @Override
    public Uni<Boolean> setnx(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> async.setnx(key, value));
    }

    @Override
    public Uni<Long> setrange(K key, long offset, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        positiveOrZero(offset, "offset");
        return LettuceResult.toUni(() -> async.setrange(key, offset, value));
    }

    @Override
    public Uni<Long> strlen(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> async.strlen(key));
    }

    private static boolean isOk(String response) {
        return "OK".equals(response);
    }

    private static <K, V> void requireNonEmpty(Map<K, V> map, String name) {
        if (map == null || map.isEmpty()) {
            throw new IllegalArgumentException("`" + name + "` must not be null or empty");
        }
    }
}
