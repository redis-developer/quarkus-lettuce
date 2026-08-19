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
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.hash.ReactiveHashCommands;
import io.quarkus.redis.datasource.hash.ReactiveHashScanCursor;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveHashCommands}.
 * <p>
 * Lettuce models the hash commands as {@link RedisHashAsyncCommands}, whose <em>key</em> type
 * parameter covers both the Redis key and the hash fields — it has no separate field type. The
 * field type {@code <F>} is therefore bound to that parameter and the Redis key is routed through
 * it as well (see {@link #asField(Object)}), so both are encoded by the connection codec's key
 * codec. That is only correct when {@code <K>} and {@code <F>} are the same type, which the
 * constructor enforces — a mismatch is rejected with an {@link IllegalArgumentException}.
 *
 * @param <K> the key type
 * @param <F> the field type
 * @param <V> the value type
 */
public class LettuceReactiveHashCommandsImpl<K, F, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveHashCommands<K, F, V> {

    private final ReactiveRedisDataSource dataSource;

    @SuppressWarnings("unchecked")
    private final RedisHashAsyncCommands<F, V> hash = (RedisHashAsyncCommands<F, V>) async;

    public LettuceReactiveHashCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection, Type keyType, Type fieldType) {
        super(connection);
        if (!keyType.equals(fieldType)) {
            throw new IllegalArgumentException("The Lettuce backend requires the hash field type to be the same as"
                    + " the Redis key type, because Lettuce's hash commands encode both with the connection codec's"
                    + " key codec. Got key type `" + keyType.getTypeName() + "` and field type `"
                    + fieldType.getTypeName() + "`. Use quarkus.redis.backend=vertx for differing types.");
        }
        this.dataSource = dataSource;
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
        return () -> hash.hdel(asField(key), fields);
    }

    @Override
    public Uni<Boolean> hexists(K key, F field) {
        return LettuceResult.toUni(_hexists(key, field));
    }

    Supplier<RedisFuture<Boolean>> _hexists(K key, F field) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> hash.hexists(asField(key), field);
    }

    @Override
    public Uni<V> hget(K key, F field) {
        return LettuceResult.toUni(_hget(key, field));
    }

    Supplier<RedisFuture<V>> _hget(K key, F field) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> hash.hget(asField(key), field);
    }

    @Override
    public Uni<Long> hincrby(K key, F field, long amount) {
        return LettuceResult.toUni(_hincrby(key, field, amount));
    }

    Supplier<RedisFuture<Long>> _hincrby(K key, F field, long amount) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> hash.hincrby(asField(key), field, amount);
    }

    @Override
    public Uni<Double> hincrbyfloat(K key, F field, double amount) {
        return LettuceResult.toUni(_hincrbyfloat(key, field, amount));
    }

    Supplier<RedisFuture<Double>> _hincrbyfloat(K key, F field, double amount) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> hash.hincrbyfloat(asField(key), field, amount);
    }

    @Override
    public Uni<Map<F, V>> hgetall(K key) {
        return LettuceResult.toUni(_hgetall(key));
    }

    Supplier<RedisFuture<Map<F, V>>> _hgetall(K key) {
        nonNull(key, "key");
        return () -> hash.hgetall(asField(key));
    }

    @Override
    public Uni<List<F>> hkeys(K key) {
        return LettuceResult.toUni(_hkeys(key));
    }

    Supplier<RedisFuture<List<F>>> _hkeys(K key) {
        nonNull(key, "key");
        return () -> hash.hkeys(asField(key));
    }

    @Override
    public Uni<Long> hlen(K key) {
        return LettuceResult.toUni(_hlen(key));
    }

    Supplier<RedisFuture<Long>> _hlen(K key) {
        nonNull(key, "key");
        return () -> hash.hlen(asField(key));
    }

    @SafeVarargs
    @Override
    public final Uni<Map<F, V>> hmget(K key, F... fields) {
        return LettuceResult.toUni(_hmget(key, fields)).map(this::toMap);
    }

    /**
     * Empty {@code fields} fails the returned {@code Uni} on subscription instead of throwing here.
     */
    @SafeVarargs
    final Supplier<RedisFuture<List<KeyValue<F, V>>>> _hmget(K key, F... fields) {
        nonNull(key, "key");
        doesNotContainNull(fields, "fields");
        if (fields.length == 0) {
            return () -> {
                throw new IllegalArgumentException("`fields` must not be empty");
            };
        }
        return () -> hash.hmget(asField(key), fields);
    }

    @Deprecated
    @Override
    public Uni<Void> hmset(K key, Map<F, V> map) {
        return LettuceResult.toUni(_hmset(key, map)).replaceWithVoid();
    }

    /**
     * Empty {@code map} fails the returned {@code Uni} on subscription instead of throwing here.
     */
    Supplier<RedisFuture<String>> _hmset(K key, Map<F, V> map) {
        nonNull(key, "key");
        nonNull(map, "map");
        if (map.isEmpty()) {
            return () -> {
                throw new IllegalArgumentException("`map` must not be empty");
            };
        }
        return () -> hash.hmset(asField(key), map);
    }

    @Override
    public Uni<F> hrandfield(K key) {
        return LettuceResult.toUni(_hrandfield(key));
    }

    Supplier<RedisFuture<F>> _hrandfield(K key) {
        nonNull(key, "key");
        return () -> hash.hrandfield(asField(key));
    }

    @Override
    public Uni<List<F>> hrandfield(K key, long count) {
        return LettuceResult.toUni(_hrandfield(key, count));
    }

    Supplier<RedisFuture<List<F>>> _hrandfield(K key, long count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> hash.hrandfield(asField(key), count);
    }

    @Override
    public Uni<Map<F, V>> hrandfieldWithValues(K key, long count) {
        return LettuceResult.toUni(_hrandfieldWithValues(key, count)).map(this::toMap);
    }

    Supplier<RedisFuture<List<KeyValue<F, V>>>> _hrandfieldWithValues(K key, long count) {
        nonNull(key, "key");
        return () -> hash.hrandfieldWithvalues(asField(key), count);
    }

    @Override
    public ReactiveHashScanCursor<F, V> hscan(K key) {
        nonNull(key, "key");
        return new LettuceReactiveHashScanCursorImpl<>(hash, asField(key));
    }

    @Override
    public ReactiveHashScanCursor<F, V> hscan(K key, ScanArgs scanArgs) {
        nonNull(key, "key");
        nonNull(scanArgs, "scanArgs");
        return new LettuceReactiveHashScanCursorImpl<>(hash, asField(key),
                LettuceHashCommandsConverters.toLettuceScanArgs(scanArgs));
    }

    @Override
    public Uni<Boolean> hset(K key, F field, V value) {
        return LettuceResult.toUni(_hset(key, field, value));
    }

    Supplier<RedisFuture<Boolean>> _hset(K key, F field, V value) {
        nonNull(key, "key");
        nonNull(field, "field");
        nonNull(value, "value");
        return () -> hash.hset(asField(key), field, value);
    }

    @Override
    public Uni<Long> hset(K key, Map<F, V> map) {
        return LettuceResult.toUni(_hset(key, map));
    }

    /**
     * Empty {@code map} fails the returned {@code Uni} on subscription instead of throwing here.
     */
    Supplier<RedisFuture<Long>> _hset(K key, Map<F, V> map) {
        nonNull(key, "key");
        nonNull(map, "map");
        if (map.isEmpty()) {
            return () -> {
                throw new IllegalArgumentException("`map` must not be empty");
            };
        }
        return () -> hash.hset(asField(key), map);
    }

    @Override
    public Uni<Boolean> hsetnx(K key, F field, V value) {
        return LettuceResult.toUni(_hsetnx(key, field, value));
    }

    Supplier<RedisFuture<Boolean>> _hsetnx(K key, F field, V value) {
        nonNull(key, "key");
        nonNull(field, "field");
        nonNull(value, "value");
        return () -> hash.hsetnx(asField(key), field, value);
    }

    @Override
    public Uni<Long> hstrlen(K key, F field) {
        return LettuceResult.toUni(_hstrlen(key, field));
    }

    Supplier<RedisFuture<Long>> _hstrlen(K key, F field) {
        nonNull(key, "key");
        nonNull(field, "field");
        return () -> hash.hstrlen(asField(key), field);
    }

    @Override
    public Uni<List<V>> hvals(K key) {
        return LettuceResult.toUni(_hvals(key));
    }

    Supplier<RedisFuture<List<V>>> _hvals(K key) {
        nonNull(key, "key");
        return () -> hash.hvals(asField(key));
    }

    /**
     * Routes the Redis key through the field codec. The cast is safe because the constructor rejects
     * a field type that differs from the key type (see class Javadoc).
     */
    @SuppressWarnings("unchecked")
    private F asField(K key) {
        return (F) key;
    }

    /**
     * Collapses Lettuce's {@code List<KeyValue>} (from hmget / hrandfield-with-values) into a map.
     */
    Map<F, V> toMap(List<KeyValue<F, V>> entries) {
        Map<F, V> result = new LinkedHashMap<>();
        for (KeyValue<F, V> entry : entries) {
            result.put(entry.getKey(), entry.getValueOrElse(null));
        }
        return result;
    }
}
