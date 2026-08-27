package io.quarkus.redis.runtime.client.lettuce.value;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;
import static io.smallrye.mutiny.helpers.ParameterValidation.positiveOrZero;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StringMatchResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.StringMatchResultOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.string.ReactiveStringCommands;
import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.quarkus.redis.runtime.datasource.Marshaller;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveStringCommands} and {@link ReactiveValueCommands}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LettuceReactiveValueCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveStringCommands<K, V>, ReactiveValueCommands<K, V> {

    private final ReactiveRedisDataSource dataSource;

    public LettuceReactiveValueCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<byte[], byte[]> connection, Type keyType, Type valueType) {
        super(connection, keyType, valueType, new Marshaller(keyType, valueType));
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

    Supplier<RedisFuture<Long>> _append(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.append(marshaller.encode(key), marshaller.encode(value));
    }

    @Override
    public Uni<Long> decr(K key) {
        return LettuceResult.toUni(_decr(key));
    }

    Supplier<RedisFuture<Long>> _decr(K key) {
        nonNull(key, "key");
        return () -> async.decr(marshaller.encode(key));
    }

    @Override
    public Uni<Long> decrby(K key, long amount) {
        return LettuceResult.toUni(_decrby(key, amount));
    }

    Supplier<RedisFuture<Long>> _decrby(K key, long amount) {
        nonNull(key, "key");
        return () -> async.decrby(marshaller.encode(key), amount);
    }

    @Override
    public Uni<V> get(K key) {
        return LettuceResult.toUni(_get(key)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _get(K key) {
        nonNull(key, "key");
        return () -> async.get(marshaller.encode(key));
    }

    @Override
    public Uni<V> getdel(K key) {
        return LettuceResult.toUni(_getdel(key)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _getdel(K key) {
        nonNull(key, "key");
        return () -> async.getdel(marshaller.encode(key));
    }

    @Override
    public Uni<V> getex(K key, GetExArgs args) {
        return LettuceResult.toUni(_getex(key, args)).map(this::decodeV);
    }

    @Override
    public Uni<V> getex(K key, io.quarkus.redis.datasource.string.GetExArgs args) {
        return getex(key, (GetExArgs) args);
    }

    Supplier<RedisFuture<byte[]>> _getex(K key, GetExArgs args) {
        nonNull(key, "key");
        nonNull(args, "args");
        io.lettuce.core.GetExArgs lettuceArgs = LettuceValueCommandsConverters.toLettuceGetExArgs(args);
        return () -> async.getex(marshaller.encode(key), lettuceArgs);
    }

    @Override
    public Uni<String> getrange(K key, long start, long end) {
        return LettuceResult.toUni(_getrange(key, start, end)).map(this::decodeString);
    }

    Supplier<RedisFuture<byte[]>> _getrange(K key, long start, long end) {
        nonNull(key, "key");
        positiveOrZero(start, "start");
        return () -> async.getrange(marshaller.encode(key), start, end);
    }

    @Deprecated
    @Override
    public Uni<V> getset(K key, V value) {
        return LettuceResult.toUni(_getset(key, value)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _getset(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.getset(marshaller.encode(key), marshaller.encode(value));
    }

    @Override
    public Uni<Long> incr(K key) {
        return LettuceResult.toUni(_incr(key));
    }

    Supplier<RedisFuture<Long>> _incr(K key) {
        nonNull(key, "key");
        return () -> async.incr(marshaller.encode(key));
    }

    @Override
    public Uni<Long> incrby(K key, long amount) {
        return LettuceResult.toUni(_incrby(key, amount));
    }

    Supplier<RedisFuture<Long>> _incrby(K key, long amount) {
        nonNull(key, "key");
        return () -> async.incrby(marshaller.encode(key), amount);
    }

    @Override
    public Uni<Double> incrbyfloat(K key, double amount) {
        return LettuceResult.toUni(_incrbyfloat(key, amount));
    }

    Supplier<RedisFuture<Double>> _incrbyfloat(K key, double amount) {
        nonNull(key, "key");
        return () -> async.incrbyfloat(marshaller.encode(key), amount);
    }

    @Override
    public Uni<String> lcs(K key1, K key2) {
        return LettuceResult.toUni(_lcs(key1, key2))
                .map(r -> r == null ? null : r.getMatchString());
    }

    Supplier<RedisFuture<StringMatchResult>> _lcs(K key1, K key2) {
        nonNull(key1, "key1");
        nonNull(key2, "key2");
        return () -> dispatchLcs(key1, key2, false);
    }

    @Override
    public Uni<Long> lcsLength(K key1, K key2) {
        return LettuceResult.toUni(_lcsLength(key1, key2))
                .map(r -> r == null ? null : r.getLen());
    }

    Supplier<RedisFuture<StringMatchResult>> _lcsLength(K key1, K key2) {
        nonNull(key1, "key1");
        nonNull(key2, "key2");
        return () -> dispatchLcs(key1, key2, true);
    }

    /**
     * Dispatches {@code LCS} as a raw command. Lettuce's own {@code lcs(...)} decodes the match
     * string with the connection codec's value codec and blind-casts it to {@code String}, which
     * fails on the {@code byte[]} codec. Dispatching with a String-codec-backed output decodes
     * the match string as UTF-8, while the keys still go out marshaller-encoded like every other
     * command in this group.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RedisFuture<StringMatchResult> dispatchLcs(K key1, K key2, boolean justLen) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE)
                .addKey(marshaller.encode(key1))
                .addKey(marshaller.encode(key2));
        if (justLen) {
            args.add("LEN");
        }
        CommandOutput<byte[], byte[], StringMatchResult> output = (CommandOutput) new StringMatchResultOutput<>(
                StringCodec.UTF8);
        return async.dispatch(CommandType.LCS, output, args);
    }

    @SafeVarargs
    @Override
    public final Uni<Map<K, V>> mget(K... keys) {
        return LettuceResult.toUni(_mget(keys)).map(r -> decodeAsOrderedMap(keys, r));
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<KeyValue<byte[], byte[]>>>> _mget(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        return () -> async.mget(marshaller.encodeAsArray(keys));
    }

    @Override
    public Uni<Void> mset(Map<K, V> map) {
        return LettuceResult.toUni(_mset(map)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _mset(Map<K, V> map) {
        notNullOrEmpty(map, "map");
        return () -> async.mset(encodeMap(map));
    }

    @Override
    public Uni<Boolean> msetnx(Map<K, V> map) {
        return LettuceResult.toUni(_msetnx(map));
    }

    Supplier<RedisFuture<Boolean>> _msetnx(Map<K, V> map) {
        notNullOrEmpty(map, "map");
        return () -> async.msetnx(encodeMap(map));
    }

    @Override
    public Uni<Void> psetex(K key, long milliseconds, V value) {
        return LettuceResult.toUni(_psetex(key, milliseconds, value)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _psetex(K key, long milliseconds, V value) {
        nonNull(key, "key");
        positive(milliseconds, "milliseconds");
        nonNull(value, "value");
        return () -> async.psetex(marshaller.encode(key), milliseconds, marshaller.encode(value));
    }

    @Override
    public Uni<Void> set(K key, V value) {
        return LettuceResult.toUni(_set(key, value)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _set(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.set(marshaller.encode(key), marshaller.encode(value));
    }

    @Override
    public Uni<Void> set(K key, V value, SetArgs setArgs) {
        return LettuceResult.toUni(_set(key, value, setArgs)).replaceWithVoid();
    }

    @Override
    public Uni<Void> set(K key, V value, io.quarkus.redis.datasource.string.SetArgs setArgs) {
        // The upcast routes to the value-args overload; without it this call would recurse.
        return set(key, value, (SetArgs) setArgs);
    }

    Supplier<RedisFuture<String>> _set(K key, V value, SetArgs setArgs) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(setArgs, "setArgs");
        io.lettuce.core.SetArgs lettuceArgs = LettuceValueCommandsConverters.toLettuceSetArgs(setArgs);
        return () -> async.set(marshaller.encode(key), marshaller.encode(value), lettuceArgs);
    }

    @Override
    public Uni<Boolean> setAndChanged(K key, V value) {
        return LettuceResult.toUni(_set(key, value)).map(AbstractLettuceCommands::isOk);
    }

    @Override
    public Uni<Boolean> setAndChanged(K key, V value, SetArgs setArgs) {
        return LettuceResult.toUni(_set(key, value, setArgs))
                .map(AbstractLettuceCommands::isOk);
    }

    @Override
    public Uni<V> setGet(K key, V value) {
        return LettuceResult.toUni(_setGet(key, value)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _setGet(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.setGet(marshaller.encode(key), marshaller.encode(value));
    }

    @Override
    public Uni<V> setGet(K key, V value, SetArgs setArgs) {
        return LettuceResult.toUni(_setGet(key, value, setArgs)).map(this::decodeV);
    }

    @Override
    public Uni<V> setGet(K key, V value, io.quarkus.redis.datasource.string.SetArgs setArgs) {
        // The upcast routes to the value-args overload; without it this call would recurse.
        return setGet(key, value, (SetArgs) setArgs);
    }

    Supplier<RedisFuture<byte[]>> _setGet(K key, V value, SetArgs setArgs) {
        nonNull(key, "key");
        nonNull(value, "value");
        nonNull(setArgs, "setArgs");
        io.lettuce.core.SetArgs lettuceArgs = LettuceValueCommandsConverters.toLettuceSetArgs(setArgs);
        return () -> async.setGet(marshaller.encode(key), marshaller.encode(value), lettuceArgs);
    }

    @Override
    public Uni<Void> setex(K key, long seconds, V value) {
        return LettuceResult.toUni(_setex(key, seconds, value)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _setex(K key, long seconds, V value) {
        nonNull(key, "key");
        positive(seconds, "seconds");
        nonNull(value, "value");
        return () -> async.setex(marshaller.encode(key), seconds, marshaller.encode(value));
    }

    @Override
    public Uni<Boolean> setnx(K key, V value) {
        return LettuceResult.toUni(_setnx(key, value));
    }

    Supplier<RedisFuture<Boolean>> _setnx(K key, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        return () -> async.setnx(marshaller.encode(key), marshaller.encode(value));
    }

    @Override
    public Uni<Long> setrange(K key, long offset, V value) {
        return LettuceResult.toUni(_setrange(key, offset, value));
    }

    Supplier<RedisFuture<Long>> _setrange(K key, long offset, V value) {
        nonNull(key, "key");
        nonNull(value, "value");
        positiveOrZero(offset, "offset");
        return () -> async.setrange(marshaller.encode(key), offset, marshaller.encode(value));
    }

    @Override
    public Uni<Long> strlen(K key) {
        return LettuceResult.toUni(_strlen(key));
    }

    Supplier<RedisFuture<Long>> _strlen(K key) {
        nonNull(key, "key");
        return () -> async.strlen(marshaller.encode(key));
    }

}
