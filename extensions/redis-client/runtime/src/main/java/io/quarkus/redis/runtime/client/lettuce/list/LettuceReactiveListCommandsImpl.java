package io.quarkus.redis.runtime.client.lettuce.list;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.quarkus.redis.runtime.datasource.Validation.positiveOrZero;
import static io.quarkus.redis.runtime.datasource.Validation.validateTimeout;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.KeyValue;
import io.quarkus.redis.datasource.list.LPosArgs;
import io.quarkus.redis.datasource.list.Position;
import io.quarkus.redis.datasource.list.ReactiveListCommands;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveListCommands}, on top of
 * {@link RedisListAsyncCommands} plus {@link RedisKeyAsyncCommands} for {@code SORT}.
 * <p>
 * Deviations from the Vert.x backend:
 * <ul>
 * <li>{@code LMPOP} / {@code BLMPOP} reply as one {@code KeyValue<K, List<V>>} in Lettuce, collapsed
 * to the API's shapes by {@link #toFirstKeyValue} and {@link #toKeyValueList}.</li>
 * <li>A {@code nil} list reply decodes to an empty list, see {@link #orEmpty}.</li>
 * <li>{@code rpop(key, count)} leaves {@code count} unvalidated; only {@code lpop} validates it.</li>
 * <li>Blocking timeouts are sent as whole seconds unless the {@link Duration} is sub-second
 * (fractional timeouts need Redis 6).</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LettuceReactiveListCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveListCommands<K, V> {

    private static final SortArgs DEFAULT_SORT_ARGS = new SortArgs();

    private final ReactiveRedisDataSource dataSource;

    private final RedisListAsyncCommands<K, V> list = async;

    /**
     * {@code SORT} lives in Lettuce's key commands, not its list commands.
     */
    private final RedisKeyAsyncCommands<K, V> sortable = async;

    public LettuceReactiveListCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection) {
        super(connection);
        this.dataSource = dataSource;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<V> blmove(K source, K destination, Position positionInSource, Position positionInDest, Duration timeout) {
        return LettuceResult.toUni(_blmove(source, destination, positionInSource, positionInDest, timeout));
    }

    Supplier<RedisFuture<V>> _blmove(K source, K destination, Position positionInSource, Position positionInDest,
            Duration timeout) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        nonNull(positionInSource, "positionInSource");
        nonNull(positionInDest, "positionInDest");
        validateTimeout(timeout, "timeout");
        io.lettuce.core.LMoveArgs args = LettuceListCommandsConverters.toLettuceLMoveArgs(positionInSource, positionInDest);
        return () -> {
            if (isWholeSeconds(timeout)) {
                return list.blmove(source, destination, args, timeout.getSeconds());
            }
            return list.blmove(source, destination, args, toFractionalSeconds(timeout));
        };
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, V>> blmpop(Duration timeout, Position position, K... keys) {
        return LettuceResult.toUni(_blmpop(timeout, position, keys)).map(this::toFirstKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, List<V>>>> _blmpop(Duration timeout, Position position,
            K... keys) {
        nonNull(position, "position");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(position);
        return () -> {
            if (isWholeSeconds(timeout)) {
                return list.blmpop(timeout.getSeconds(), args, keys);
            }
            return list.blmpop(toFractionalSeconds(timeout), args, keys);
        };
    }

    @SafeVarargs
    @Override
    public final Uni<List<KeyValue<K, V>>> blmpop(Duration timeout, Position position, int count, K... keys) {
        return LettuceResult.toUni(_blmpop(timeout, position, count, keys)).map(this::toKeyValueList);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, List<V>>>> _blmpop(Duration timeout, Position position,
            int count, K... keys) {
        nonNull(position, "position");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        positive(count, "count");
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(position, count);
        return () -> {
            if (isWholeSeconds(timeout)) {
                return list.blmpop(timeout.getSeconds(), args, keys);
            }
            return list.blmpop(toFractionalSeconds(timeout), args, keys);
        };
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, V>> blpop(Duration timeout, K... keys) {
        return LettuceResult.toUni(_blpop(timeout, keys)).map(this::toKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, V>>> _blpop(Duration timeout, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        return () -> {
            if (isWholeSeconds(timeout)) {
                return list.blpop(timeout.getSeconds(), keys);
            }
            return list.blpop(toFractionalSeconds(timeout), keys);
        };
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, V>> brpop(Duration timeout, K... keys) {
        return LettuceResult.toUni(_brpop(timeout, keys)).map(this::toKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, V>>> _brpop(Duration timeout, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        return () -> {
            if (isWholeSeconds(timeout)) {
                return list.brpop(timeout.getSeconds(), keys);
            }
            return list.brpop(toFractionalSeconds(timeout), keys);
        };
    }

    @Deprecated
    @Override
    public Uni<V> brpoplpush(Duration timeout, K source, K destination) {
        return LettuceResult.toUni(_brpoplpush(timeout, source, destination));
    }

    Supplier<RedisFuture<V>> _brpoplpush(Duration timeout, K source, K destination) {
        validateTimeout(timeout, "timeout");
        nonNull(source, "source");
        nonNull(destination, "destination");
        return () -> {
            if (isWholeSeconds(timeout)) {
                return list.brpoplpush(timeout.getSeconds(), source, destination);
            }
            return list.brpoplpush(toFractionalSeconds(timeout), source, destination);
        };
    }

    @Override
    public Uni<V> lindex(K key, long index) {
        return LettuceResult.toUni(_lindex(key, index));
    }

    Supplier<RedisFuture<V>> _lindex(K key, long index) {
        nonNull(key, "key");
        return () -> list.lindex(key, index);
    }

    @Override
    public Uni<Long> linsertBeforePivot(K key, V pivot, V element) {
        return LettuceResult.toUni(_linsertBeforePivot(key, pivot, element));
    }

    Supplier<RedisFuture<Long>> _linsertBeforePivot(K key, V pivot, V element) {
        nonNull(key, "key");
        nonNull(pivot, "pivot");
        nonNull(element, "element");
        return () -> list.linsert(key, true, pivot, element);
    }

    @Override
    public Uni<Long> linsertAfterPivot(K key, V pivot, V element) {
        return LettuceResult.toUni(_linsertAfterPivot(key, pivot, element));
    }

    Supplier<RedisFuture<Long>> _linsertAfterPivot(K key, V pivot, V element) {
        nonNull(key, "key");
        nonNull(pivot, "pivot");
        nonNull(element, "element");
        return () -> list.linsert(key, false, pivot, element);
    }

    @Override
    public Uni<Long> llen(K key) {
        return LettuceResult.toUni(_llen(key));
    }

    Supplier<RedisFuture<Long>> _llen(K key) {
        nonNull(key, "key");
        return () -> list.llen(key);
    }

    @Override
    public Uni<V> lmove(K source, K destination, Position positionInSource, Position positionInDestination) {
        return LettuceResult.toUni(_lmove(source, destination, positionInSource, positionInDestination));
    }

    Supplier<RedisFuture<V>> _lmove(K source, K destination, Position positionInSource, Position positionInDest) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        nonNull(positionInSource, "positionInSource");
        nonNull(positionInDest, "positionInDest");
        io.lettuce.core.LMoveArgs args = LettuceListCommandsConverters.toLettuceLMoveArgs(positionInSource, positionInDest);
        return () -> list.lmove(source, destination, args);
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, V>> lmpop(Position position, K... keys) {
        return LettuceResult.toUni(_lmpop(position, keys)).map(this::toFirstKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, List<V>>>> _lmpop(Position position, K... keys) {
        nonNull(position, "position");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(position);
        return () -> list.lmpop(args, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<List<KeyValue<K, V>>> lmpop(Position position, int count, K... keys) {
        return LettuceResult.toUni(_lmpop(position, count, keys)).map(this::toKeyValueList);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<K, List<V>>>> _lmpop(Position position, int count, K... keys) {
        nonNull(position, "position");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        positive(count, "count");
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(position, count);
        return () -> list.lmpop(args, keys);
    }

    @Override
    public Uni<V> lpop(K key) {
        return LettuceResult.toUni(_lpop(key));
    }

    Supplier<RedisFuture<V>> _lpop(K key) {
        nonNull(key, "key");
        return () -> list.lpop(key);
    }

    @Override
    public Uni<List<V>> lpop(K key, int count) {
        return LettuceResult.toUni(_lpop(key, count)).map(LettuceReactiveListCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _lpop(K key, int count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> list.lpop(key, count);
    }

    @Override
    public Uni<Long> lpos(K key, V element) {
        return LettuceResult.toUni(_lpos(key, element));
    }

    Supplier<RedisFuture<Long>> _lpos(K key, V element) {
        nonNull(key, "key");
        nonNull(element, "element");
        return () -> list.lpos(key, element);
    }

    @Override
    public Uni<Long> lpos(K key, V element, LPosArgs args) {
        return LettuceResult.toUni(_lpos(key, element, args));
    }

    Supplier<RedisFuture<Long>> _lpos(K key, V element, LPosArgs args) {
        nonNull(key, "key");
        nonNull(element, "element");
        io.lettuce.core.LPosArgs lettuceArgs = LettuceListCommandsConverters.toLettuceLPosArgs(args);
        return () -> list.lpos(key, element, lettuceArgs);
    }

    @Override
    public Uni<List<Long>> lpos(K key, V element, int count) {
        return LettuceResult.toUni(_lpos(key, element, count)).map(LettuceReactiveListCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<Long>>> _lpos(K key, V element, int count) {
        nonNull(key, "key");
        nonNull(element, "element");
        positiveOrZero(count, "count"); // 0 -> All matches
        return () -> list.lpos(key, element, count);
    }

    @Override
    public Uni<List<Long>> lpos(K key, V element, int count, LPosArgs args) {
        return LettuceResult.toUni(_lpos(key, element, count, args)).map(LettuceReactiveListCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<Long>>> _lpos(K key, V element, int count, LPosArgs args) {
        nonNull(key, "key");
        nonNull(element, "element");
        positiveOrZero(count, "count"); // 0 -> All matches
        io.lettuce.core.LPosArgs lettuceArgs = LettuceListCommandsConverters.toLettuceLPosArgs(args);
        return () -> list.lpos(key, element, count, lettuceArgs);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> lpush(K key, V... elements) {
        return LettuceResult.toUni(_lpush(key, elements));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _lpush(K key, V... elements) {
        nonNull(key, "key");
        notNullOrEmpty(elements, "elements");
        doesNotContainNull(elements, "elements");
        return () -> list.lpush(key, elements);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> lpushx(K key, V... elements) {
        return LettuceResult.toUni(_lpushx(key, elements));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _lpushx(K key, V... elements) {
        nonNull(key, "key");
        notNullOrEmpty(elements, "elements");
        doesNotContainNull(elements, "elements");
        return () -> list.lpushx(key, elements);
    }

    @Override
    public Uni<List<V>> lrange(K key, long start, long stop) {
        return LettuceResult.toUni(_lrange(key, start, stop)).map(LettuceReactiveListCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _lrange(K key, long start, long stop) {
        nonNull(key, "key");
        return () -> list.lrange(key, start, stop);
    }

    @Override
    public Uni<Long> lrem(K key, long count, V element) {
        return LettuceResult.toUni(_lrem(key, count, element));
    }

    Supplier<RedisFuture<Long>> _lrem(K key, long count, V element) {
        nonNull(key, "key");
        nonNull(element, "element");
        return () -> list.lrem(key, count, element);
    }

    @Override
    public Uni<Void> lset(K key, long index, V element) {
        return LettuceResult.toUni(_lset(key, index, element)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _lset(K key, long index, V element) {
        nonNull(key, "key");
        nonNull(element, "element");
        return () -> list.lset(key, index, element);
    }

    @Override
    public Uni<Void> ltrim(K key, long start, long stop) {
        return LettuceResult.toUni(_ltrim(key, start, stop)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _ltrim(K key, long start, long stop) {
        nonNull(key, "key");
        return () -> list.ltrim(key, start, stop);
    }

    @Override
    public Uni<V> rpop(K key) {
        return LettuceResult.toUni(_rpop(key));
    }

    Supplier<RedisFuture<V>> _rpop(K key) {
        nonNull(key, "key");
        return () -> list.rpop(key);
    }

    @Override
    public Uni<List<V>> rpop(K key, int count) {
        return LettuceResult.toUni(_rpop(key, count)).map(LettuceReactiveListCommandsImpl::orEmpty);
    }

    /**
     * {@code count} is unvalidated, as in the Vert.x backend: Redis rejects a non-positive one.
     */
    Supplier<RedisFuture<List<V>>> _rpop(K key, int count) {
        nonNull(key, "key");
        return () -> list.rpop(key, count);
    }

    @Deprecated
    @Override
    public Uni<V> rpoplpush(K source, K destination) {
        return LettuceResult.toUni(_rpoplpush(source, destination));
    }

    Supplier<RedisFuture<V>> _rpoplpush(K source, K destination) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        return () -> list.rpoplpush(source, destination);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> rpush(K key, V... values) {
        return LettuceResult.toUni(_rpush(key, values));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _rpush(K key, V... values) {
        nonNull(key, "key");
        notNullOrEmpty(values, "values");
        doesNotContainNull(values, "values");
        return () -> list.rpush(key, values);
    }

    @SafeVarargs
    @Override
    public final Uni<Long> rpushx(K key, V... values) {
        return LettuceResult.toUni(_rpushx(key, values));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _rpushx(K key, V... values) {
        nonNull(key, "key");
        notNullOrEmpty(values, "values");
        doesNotContainNull(values, "values");
        return () -> list.rpushx(key, values);
    }

    @Override
    public Uni<List<V>> sort(K key) {
        return sort(key, DEFAULT_SORT_ARGS);
    }

    @Override
    public Uni<List<V>> sort(K key, SortArgs sortArguments) {
        return LettuceResult.toUni(_sort(key, sortArguments)).map(LettuceReactiveListCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _sort(K key, SortArgs sortArguments) {
        nonNull(key, "key");
        nonNull(sortArguments, "sortArguments");
        io.lettuce.core.SortArgs lettuceArgs = LettuceListCommandsConverters.toLettuceSortArgs(sortArguments);
        return () -> sortable.sort(key, lettuceArgs);
    }

    @Override
    public Uni<Long> sortAndStore(K key, K destination, SortArgs sortArguments) {
        return LettuceResult.toUni(_sortAndStore(key, destination, sortArguments));
    }

    Supplier<RedisFuture<Long>> _sortAndStore(K key, K destination, SortArgs args) {
        nonNull(key, "key");
        nonNull(destination, "destination");
        nonNull(args, "args");
        io.lettuce.core.SortArgs lettuceArgs = LettuceListCommandsConverters.toLettuceSortArgs(args);
        return () -> sortable.sortStore(key, lettuceArgs, destination);
    }

    @Override
    public Uni<Long> sortAndStore(K key, K destination) {
        return sortAndStore(key, destination, DEFAULT_SORT_ARGS);
    }

    /**
     * Adapts a Lettuce {@code KeyValue}; an empty reply (pop timeout, missing key) gives {@code null}.
     */
    KeyValue<K, V> toKeyValue(io.lettuce.core.KeyValue<K, V> kv) {
        if (kv == null || kv.isEmpty()) {
            return null;
        }
        return KeyValue.of(kv.getKey(), kv.getValueOrElse(null));
    }

    /**
     * First element of an {@code LMPOP} reply — what the count-less overloads return.
     */
    KeyValue<K, V> toFirstKeyValue(io.lettuce.core.KeyValue<K, List<V>> kv) {
        if (kv == null || kv.isEmpty()) {
            return null;
        }
        List<V> values = kv.getValue();
        if (values == null || values.isEmpty()) {
            return null;
        }
        return KeyValue.of(kv.getKey(), values.get(0));
    }

    /**
     * One {@link KeyValue} per element of an {@code LMPOP} reply — what the {@code count} overloads return.
     */
    List<KeyValue<K, V>> toKeyValueList(io.lettuce.core.KeyValue<K, List<V>> kv) {
        if (kv == null || kv.isEmpty()) {
            return Collections.emptyList();
        }
        List<V> values = kv.getValue();
        if (values == null) {
            return Collections.emptyList();
        }
        List<KeyValue<K, V>> result = new ArrayList<>(values.size());
        for (V value : values) {
            result.add(KeyValue.of(kv.getKey(), value));
        }
        return result;
    }

    /**
     * Maps a {@code nil} list reply to an empty list, as the Vert.x marshaller does.
     */
    static <T> List<T> orEmpty(List<T> values) {
        return values == null ? Collections.emptyList() : values;
    }

    private static boolean isWholeSeconds(Duration duration) {
        return duration.getNano() == 0;
    }

    private static double toFractionalSeconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }
}
