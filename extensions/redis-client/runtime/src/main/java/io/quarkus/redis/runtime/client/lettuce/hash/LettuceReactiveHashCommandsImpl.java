package io.quarkus.redis.runtime.client.lettuce.hash;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.quarkus.redis.runtime.datasource.Validation.positive;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.hash.ReactiveHashCommands;
import io.quarkus.redis.datasource.hash.ReactiveHashScanCursor;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.quarkus.redis.runtime.datasource.Marshaller;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveHashCommands}.
 *
 * @param <K> the key type
 * @param <F> the field type
 * @param <V> the value type
 */
public class LettuceReactiveHashCommandsImpl<K, F, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveHashCommands<K, F, V> {

    private final ReactiveRedisDataSource dataSource;
    private final Type fieldType;

    public LettuceReactiveHashCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<byte[], byte[]> connection, Type keyType, Type fieldType, Type valueType) {
        super(connection, keyType, valueType, new Marshaller(keyType, fieldType, valueType));
        this.dataSource = dataSource;
        this.fieldType = fieldType;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> hdel(K key, F... fields) {
        return LettuceResult.toUni(_hdel(key, fields)).map(Long::intValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _hdel(K key, F... fields) {
        nonNull(key, "key");
        notNullOrEmpty(fields, "fields");
        doesNotContainNull(fields, "fields");
        return () -> async.hdel(marshaller.encode(key), marshaller.encodeAsArray(fields));
    }

    @Override
    public Uni<Boolean> hexists(K key, F field) {
        return LettuceResult.toUni(_hexists(key, field));
    }

    Supplier<RedisFuture<Boolean>> _hexists(K key, F field) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> async.hexists(marshaller.encode(key), marshaller.encode(field));
    }

    @Override
    public Uni<V> hget(K key, F field) {
        return LettuceResult.toUni(_hget(key, field)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _hget(K key, F field) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> async.hget(marshaller.encode(key), marshaller.encode(field));
    }

    @Override
    public Uni<Long> hincrby(K key, F field, long amount) {
        return LettuceResult.toUni(_hincrby(key, field, amount));
    }

    Supplier<RedisFuture<Long>> _hincrby(K key, F field, long amount) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> async.hincrby(marshaller.encode(key), marshaller.encode(field), amount);
    }

    @Override
    public Uni<Double> hincrbyfloat(K key, F field, double amount) {
        return LettuceResult.toUni(_hincrbyfloat(key, field, amount));
    }

    Supplier<RedisFuture<Double>> _hincrbyfloat(K key, F field, double amount) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> async.hincrbyfloat(marshaller.encode(key), marshaller.encode(field), amount);
    }

    @Override
    public Uni<Map<F, V>> hgetall(K key) {
        return LettuceResult.toUni(_hgetall(key)).map(this::decodeMap);
    }

    Supplier<RedisFuture<Map<byte[], byte[]>>> _hgetall(K key) {
        nonNull(key, "key");
        return () -> async.hgetall(marshaller.encode(key));
    }

    @Override
    public Uni<List<F>> hkeys(K key) {
        return LettuceResult.toUni(_hkeys(key)).map(this::decodeListOfField);
    }

    Supplier<RedisFuture<List<byte[]>>> _hkeys(K key) {
        nonNull(key, "key");
        return () -> async.hkeys(marshaller.encode(key));
    }

    @Override
    public Uni<Long> hlen(K key) {
        return LettuceResult.toUni(_hlen(key));
    }

    Supplier<RedisFuture<Long>> _hlen(K key) {
        nonNull(key, "key");
        return () -> async.hlen(marshaller.encode(key));
    }

    @SafeVarargs
    @Override
    public final Uni<Map<F, V>> hmget(K key, F... fields) {
        return LettuceResult.toUni(_hmget(key, fields)).map(r -> decodeAsOrderedMap(fields, r));
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<KeyValue<byte[], byte[]>>>> _hmget(K key, F... fields) {
        nonNull(key, "key");
        doesNotContainNull(fields, "fields");
        if (fields.length == 0) {
            return () -> {
                throw new IllegalArgumentException("`fields` must not be empty");
            };
        }
        byte[][] encodedFields = marshaller.encodeAsArray(fields);
        return () -> async.hmget(marshaller.encode(key), encodedFields);
    }

    @Deprecated
    @Override
    public Uni<Void> hmset(K key, Map<F, V> map) {
        return LettuceResult.toUni(_hmset(key, map)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _hmset(K key, Map<F, V> map) {
        nonNull(key, "key");
        nonNull(map, "map");
        if (map.isEmpty()) {
            return () -> {
                throw new IllegalArgumentException("`map` must not be empty");
            };
        }
        return () -> async.hmset(marshaller.encode(key), encodeMapWithNullableValues(map));
    }

    @Override
    public Uni<F> hrandfield(K key) {
        return LettuceResult.toUni(_hrandfield(key)).map(this::decodeF);
    }

    Supplier<RedisFuture<byte[]>> _hrandfield(K key) {
        nonNull(key, "key");
        return () -> async.hrandfield(marshaller.encode(key));
    }

    @Override
    public Uni<List<F>> hrandfield(K key, long count) {
        return LettuceResult.toUni(_hrandfield(key, count)).map(this::decodeListOfField);
    }

    Supplier<RedisFuture<List<byte[]>>> _hrandfield(K key, long count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> async.hrandfield(marshaller.encode(key), count);
    }

    @Override
    public Uni<Map<F, V>> hrandfieldWithValues(K key, long count) {
        return LettuceResult.toUni(_hrandfieldWithValues(key, count)).map(this::decodeFieldWithValueMap);
    }

    Supplier<RedisFuture<List<KeyValue<byte[], byte[]>>>> _hrandfieldWithValues(K key, long count) {
        nonNull(key, "key");
        return () -> async.hrandfieldWithvalues(marshaller.encode(key), count);
    }

    @Override
    public ReactiveHashScanCursor<F, V> hscan(K key) {
        nonNull(key, "key");
        return new LettuceReactiveHashScanCursorImpl<>(async, marshaller.encode(key), this::decodeMap);
    }

    @Override
    public ReactiveHashScanCursor<F, V> hscan(K key, ScanArgs scanArgs) {
        nonNull(key, "key");
        nonNull(scanArgs, "scanArgs");
        return new LettuceReactiveHashScanCursorImpl<>(async, marshaller.encode(key),
                LettuceHashCommandsConverters.toLettuceScanArgs(scanArgs), this::decodeMap);
    }

    @Override
    public Uni<Boolean> hset(K key, F field, V value) {
        return LettuceResult.toUni(_hset(key, field, value));
    }

    Supplier<RedisFuture<Boolean>> _hset(K key, F field, V value) {
        nonNull(key, "key");
        nonNull(field, "field");
        nonNull(value, "value");
        return () -> async.hset(marshaller.encode(key), marshaller.encode(field), marshaller.encode(value));
    }

    @Override
    public Uni<Long> hset(K key, Map<F, V> map) {
        return LettuceResult.toUni(_hset(key, map));
    }

    Supplier<RedisFuture<Long>> _hset(K key, Map<F, V> map) {
        nonNull(key, "key");
        nonNull(map, "map");
        if (map.isEmpty()) {
            return () -> {
                throw new IllegalArgumentException("`map` must not be empty");
            };
        }
        return () -> async.hset(marshaller.encode(key), encodeMap(map));
    }

    @Override
    public Uni<Boolean> hsetnx(K key, F field, V value) {
        return LettuceResult.toUni(_hsetnx(key, field, value));
    }

    Supplier<RedisFuture<Boolean>> _hsetnx(K key, F field, V value) {
        nonNull(key, "key");
        nonNull(field, "field");
        nonNull(value, "value");
        return () -> async.hsetnx(marshaller.encode(key), marshaller.encode(field), marshaller.encode(value));
    }

    @Override
    public Uni<Long> hstrlen(K key, F field) {
        return LettuceResult.toUni(_hstrlen(key, field));
    }

    Supplier<RedisFuture<Long>> _hstrlen(K key, F field) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> async.hstrlen(marshaller.encode(key), marshaller.encode(field));
    }

    @Override
    public Uni<List<V>> hvals(K key) {
        return LettuceResult.toUni(_hvals(key)).map(this::decodeListOfValue);
    }

    Supplier<RedisFuture<List<byte[]>>> _hvals(K key) {
        nonNull(key, "key");
        return () -> async.hvals(marshaller.encode(key));
    }

    F decodeF(byte[] bytes) {
        return marshaller.decode(fieldType, bytes);
    }

    Map<F, V> decodeMap(Map<byte[], byte[]> map) {
        Map<F, V> decoded = new LinkedHashMap<>(map.size());
        for (Map.Entry<byte[], byte[]> e : map.entrySet()) {
            decoded.put(decodeF(e.getKey()), decodeV(e.getValue()));
        }
        return decoded;
    }

    Map<F, V> decodeFieldWithValueMap(List<KeyValue<byte[], byte[]>> entries) {
        Map<F, V> map = new LinkedHashMap<>();
        for (KeyValue<byte[], byte[]> entry : entries) {
            map.put(decodeF(entry.getKey()), decodeV(entry.getValueOrElse(null)));
        }
        return map;
    }

    List<F> decodeListOfField(List<byte[]> list) {
        return list.stream().map(this::decodeF).toList();
    }

}
