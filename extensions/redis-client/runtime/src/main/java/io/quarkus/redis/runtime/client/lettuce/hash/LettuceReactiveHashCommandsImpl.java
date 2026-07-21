package io.quarkus.redis.runtime.client.lettuce.hash;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.hash.ReactiveHashCommands;
import io.quarkus.redis.datasource.hash.ReactiveHashScanCursor;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveHashCommands}.
 * <p>
 * Quarkus separates the hash <em>field</em> type {@code F} from the key type {@code K}, unlike Lettuce's
 * {@link RedisHashAsyncCommands} which uses the single connection key codec for both the Redis key and the
 * field. This backend therefore views the async handle as {@code RedisHashAsyncCommands<F, V>}, and encodes the
 * Redis key through that same codec via {@link #asField(Object)}. The casts are safe: {@code K} and
 * {@code F} share one codec and erase to the same runtime type.
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

    static {
        LettuceHashCommandsConverters.register();
    }

    public LettuceReactiveHashCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection) {
        super(connection);
        this.dataSource = dataSource;
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> hdel(K key, F... fields) {
        nonNull(key, "key");
        notNullOrEmpty(fields, "fields");
        doesNotContainNull(fields, "fields");
        return LettuceResult.toUni(() -> hash.hdel(asField(key), fields)).map(Long::intValue);
    }

    @Override
    public Uni<Boolean> hexists(K key, F field) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> hash.hexists(asField(key), field));
    }

    @Override
    public Uni<V> hget(K key, F field) {
        nonNull(key, "key");
        nonNull(field, "field");
        return LettuceResult.toUni(() -> hash.hget(asField(key), field));
    }

    @Override
    public Uni<Long> hincrby(K key, F field, long amount) {
        nonNull(key, "key");
        nonNull(field, "field");
        return LettuceResult.toUni(() -> hash.hincrby(asField(key), field, amount));
    }

    @Override
    public Uni<Double> hincrbyfloat(K key, F field, double amount) {
        nonNull(key, "key");
        nonNull(field, "field");
        return LettuceResult.toUni(() -> hash.hincrbyfloat(asField(key), field, amount));
    }

    @Override
    public Uni<Map<F, V>> hgetall(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> hash.hgetall(asField(key)));
    }

    @Override
    public Uni<List<F>> hkeys(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> hash.hkeys(asField(key)));
    }

    @Override
    public Uni<Long> hlen(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> hash.hlen(asField(key)));
    }

    @SafeVarargs
    @Override
    public final Uni<Map<F, V>> hmget(K key, F... fields) {
        nonNull(key, "key");
        notNullOrEmpty(fields, "fields");
        doesNotContainNull(fields, "fields");
        return LettuceResult.toUni(() -> hash.hmget(asField(key), fields)).map(this::toMap);
    }

    @Override
    public Uni<Void> hmset(K key, Map<F, V> map) {
        nonNull(key, "key");
        nonNull(map, "map");
        return LettuceResult.toUni(() -> hash.hmset(asField(key), map)).replaceWithVoid();
    }

    @Override
    public Uni<F> hrandfield(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> hash.hrandfield(asField(key)));
    }

    @Override
    public Uni<List<F>> hrandfield(K key, long count) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> hash.hrandfield(asField(key), count));
    }

    @Override
    public Uni<Map<F, V>> hrandfieldWithValues(K key, long count) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> hash.hrandfieldWithvalues(asField(key), count)).map(this::toMap);
    }

    @Override
    public ReactiveHashScanCursor<F, V> hscan(K key) {
        nonNull(key, "key");
        return new LettuceHashScanCursor<>(hash, asField(key));
    }

    @Override
    public ReactiveHashScanCursor<F, V> hscan(K key, ScanArgs scanArgs) {
        nonNull(key, "key");
        nonNull(scanArgs, "scanArgs");
        return new LettuceHashScanCursor<>(hash, asField(key), LettuceConverterRegistry.convertArg(scanArgs));
    }

    @Override
    public Uni<Boolean> hset(K key, F field, V value) {
        nonNull(key, "key");
        nonNull(field, "field");
        nonNull(value, "value");
        return LettuceResult.toUni(() -> hash.hset(asField(key), field, value));
    }

    @Override
    public Uni<Long> hset(K key, Map<F, V> map) {
        nonNull(key, "key");
        nonNull(map, "map");
        if (map.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("`map` must not be empty"));
        }
        return LettuceResult.toUni(() -> hash.hset(asField(key), map));
    }

    @Override
    public Uni<Boolean> hsetnx(K key, F field, V value) {
        nonNull(key, "key");
        nonNull(field, "field");
        return LettuceResult.toUni(() -> hash.hsetnx(asField(key), field, value));
    }

    @Override
    public Uni<Long> hstrlen(K key, F field) {
        nonNull(key, "key");
        nonNull(field, "field");
        return LettuceResult.toUni(() -> hash.hstrlen(asField(key), field));
    }

    @Override
    public Uni<List<V>> hvals(K key) {
        nonNull(key, "key");
        return LettuceResult.toUni(() -> hash.hvals(asField(key)));
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    /** Routes the Redis key through the field codec (see class Javadoc). */
    @SuppressWarnings("unchecked")
    private F asField(K key) {
        return (F) key;
    }

    /** Collapses Lettuce's {@code List<KeyValue>} (from hmget / hrandfield-with-values) into a map. */
    private Map<F, V> toMap(List<KeyValue<F, V>> entries) {
        Map<F, V> result = new LinkedHashMap<>();
        for (KeyValue<F, V> entry : entries) {
            result.put(entry.getKey(), entry.getValueOrElse(null));
        }
        return result;
    }
}
