package io.quarkus.redis.runtime.client.lettuce.list;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.quarkus.redis.runtime.datasource.Validation.positiveOrZero;
import static io.quarkus.redis.runtime.datasource.Validation.validateTimeout;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.KeyValue;
import io.quarkus.redis.datasource.list.LPosArgs;
import io.quarkus.redis.datasource.list.Position;
import io.quarkus.redis.datasource.list.ReactiveListCommands;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.quarkus.redis.runtime.datasource.Marshaller;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveListCommands}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LettuceReactiveListCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveListCommands<K, V> {

    private static final SortArgs DEFAULT_SORT_ARGS = new SortArgs();

    private final ReactiveRedisDataSource dataSource;

    public LettuceReactiveListCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<byte[], byte[]> connection, Type keyType, Type valueType) {
        super(connection, keyType, valueType, new Marshaller(keyType, valueType));
        this.dataSource = dataSource;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<V> blmove(K source, K destination, Position positionInSource, Position positionInDest, Duration timeout) {
        return LettuceResult.toUni(_blmove(source, destination, positionInSource, positionInDest, timeout)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _blmove(K source, K destination, Position positionInSource, Position positionInDest,
            Duration timeout) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        nonNull(positionInSource, "positionInSource");
        nonNull(positionInDest, "positionInDest");
        validateTimeout(timeout, "timeout");
        io.lettuce.core.LMoveArgs args = LettuceListCommandsConverters.toLettuceLMoveArgs(positionInSource, positionInDest);
        return () -> {
            if (isWholeSeconds(timeout)) {
                return async.blmove(marshaller.encode(source), marshaller.encode(destination), args, timeout.getSeconds());
            }
            return async.blmove(marshaller.encode(source), marshaller.encode(destination), args, toFractionalSeconds(timeout));
        };
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, V>> blmpop(Duration timeout, Position position, K... keys) {
        return LettuceResult.toUni(_blmpop(timeout, position, keys)).map(this::toFirstKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<byte[], List<byte[]>>>> _blmpop(Duration timeout, Position position,
            K... keys) {
        nonNull(position, "position");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(position);
        return () -> {
            if (isWholeSeconds(timeout)) {
                return async.blmpop(timeout.getSeconds(), args, marshaller.encodeAsArray(keys));
            }
            return async.blmpop(toFractionalSeconds(timeout), args, marshaller.encodeAsArray(keys));
        };
    }

    @SafeVarargs
    @Override
    public final Uni<List<KeyValue<K, V>>> blmpop(Duration timeout, Position position, int count, K... keys) {
        return LettuceResult.toUni(_blmpop(timeout, position, count, keys)).map(this::toKeyValueList);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<byte[], List<byte[]>>>> _blmpop(Duration timeout, Position position,
            int count, K... keys) {
        nonNull(position, "position");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        positive(count, "count");
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(position, count);
        return () -> {
            if (isWholeSeconds(timeout)) {
                return async.blmpop(timeout.getSeconds(), args, marshaller.encodeAsArray(keys));
            }
            return async.blmpop(toFractionalSeconds(timeout), args, marshaller.encodeAsArray(keys));
        };
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, V>> blpop(Duration timeout, K... keys) {
        return LettuceResult.toUni(_blpop(timeout, keys)).map(this::toKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<byte[], byte[]>>> _blpop(Duration timeout, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        return () -> {
            if (isWholeSeconds(timeout)) {
                return async.blpop(timeout.getSeconds(), marshaller.encodeAsArray(keys));
            }
            return async.blpop(toFractionalSeconds(timeout), marshaller.encodeAsArray(keys));
        };
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, V>> brpop(Duration timeout, K... keys) {
        return LettuceResult.toUni(_brpop(timeout, keys)).map(this::toKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<byte[], byte[]>>> _brpop(Duration timeout, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        validateTimeout(timeout, "timeout");
        return () -> {
            if (isWholeSeconds(timeout)) {
                return async.brpop(timeout.getSeconds(), marshaller.encodeAsArray(keys));
            }
            return async.brpop(toFractionalSeconds(timeout), marshaller.encodeAsArray(keys));
        };
    }

    @Deprecated
    @Override
    public Uni<V> brpoplpush(Duration timeout, K source, K destination) {
        return LettuceResult.toUni(_brpoplpush(timeout, source, destination)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _brpoplpush(Duration timeout, K source, K destination) {
        validateTimeout(timeout, "timeout");
        nonNull(source, "source");
        nonNull(destination, "destination");
        return () -> {
            if (isWholeSeconds(timeout)) {
                return async.brpoplpush(timeout.getSeconds(), marshaller.encode(source), marshaller.encode(destination));
            }
            return async.brpoplpush(toFractionalSeconds(timeout), marshaller.encode(source), marshaller.encode(destination));
        };
    }

    @Override
    public Uni<V> lindex(K key, long index) {
        return LettuceResult.toUni(_lindex(key, index)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _lindex(K key, long index) {
        nonNull(key, "key");
        return () -> async.lindex(marshaller.encode(key), index);
    }

    @Override
    public Uni<Long> linsertBeforePivot(K key, V pivot, V element) {
        return LettuceResult.toUni(_linsertBeforePivot(key, pivot, element));
    }

    Supplier<RedisFuture<Long>> _linsertBeforePivot(K key, V pivot, V element) {
        nonNull(key, "key");
        nonNull(pivot, "pivot");
        nonNull(element, "element");
        return () -> async.linsert(marshaller.encode(key), true, marshaller.encode(pivot), marshaller.encode(element));
    }

    @Override
    public Uni<Long> linsertAfterPivot(K key, V pivot, V element) {
        return LettuceResult.toUni(_linsertAfterPivot(key, pivot, element));
    }

    Supplier<RedisFuture<Long>> _linsertAfterPivot(K key, V pivot, V element) {
        nonNull(key, "key");
        nonNull(pivot, "pivot");
        nonNull(element, "element");
        return () -> async.linsert(marshaller.encode(key), false, marshaller.encode(pivot), marshaller.encode(element));
    }

    @Override
    public Uni<Long> llen(K key) {
        return LettuceResult.toUni(_llen(key));
    }

    Supplier<RedisFuture<Long>> _llen(K key) {
        nonNull(key, "key");
        return () -> async.llen(marshaller.encode(key));
    }

    @Override
    public Uni<V> lmove(K source, K destination, Position positionInSource, Position positionInDestination) {
        return LettuceResult.toUni(_lmove(source, destination, positionInSource, positionInDestination))
                .map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _lmove(K source, K destination, Position positionInSource, Position positionInDest) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        nonNull(positionInSource, "positionInSource");
        nonNull(positionInDest, "positionInDest");
        io.lettuce.core.LMoveArgs args = LettuceListCommandsConverters.toLettuceLMoveArgs(positionInSource, positionInDest);
        return () -> async.lmove(marshaller.encode(source), marshaller.encode(destination), args);
    }

    @SafeVarargs
    @Override
    public final Uni<KeyValue<K, V>> lmpop(Position position, K... keys) {
        return LettuceResult.toUni(_lmpop(position, keys)).map(this::toFirstKeyValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<byte[], List<byte[]>>>> _lmpop(Position position, K... keys) {
        nonNull(position, "position");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(position);
        return () -> async.lmpop(args, marshaller.encodeAsArray(keys));
    }

    @SafeVarargs
    @Override
    public final Uni<List<KeyValue<K, V>>> lmpop(Position position, int count, K... keys) {
        return LettuceResult.toUni(_lmpop(position, count, keys)).map(this::toKeyValueList);
    }

    @SafeVarargs
    final Supplier<RedisFuture<io.lettuce.core.KeyValue<byte[], List<byte[]>>>> _lmpop(Position position, int count,
            K... keys) {
        nonNull(position, "position");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        positive(count, "count");
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(position, count);
        return () -> async.lmpop(args, marshaller.encodeAsArray(keys));
    }

    @Override
    public Uni<V> lpop(K key) {
        return LettuceResult.toUni(_lpop(key)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _lpop(K key) {
        nonNull(key, "key");
        return () -> async.lpop(marshaller.encode(key));
    }

    @Override
    public Uni<List<V>> lpop(K key, int count) {
        return LettuceResult.toUni(_lpop(key, count))
                .map(AbstractLettuceCommands::orEmpty)
                .map(this::decodeListOfValue);
    }

    Supplier<RedisFuture<List<byte[]>>> _lpop(K key, int count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> async.lpop(marshaller.encode(key), count);
    }

    @Override
    public Uni<Long> lpos(K key, V element) {
        return LettuceResult.toUni(_lpos(key, element));
    }

    Supplier<RedisFuture<Long>> _lpos(K key, V element) {
        nonNull(key, "key");
        nonNull(element, "element");
        return () -> async.lpos(marshaller.encode(key), marshaller.encode(element));
    }

    @Override
    public Uni<Long> lpos(K key, V element, LPosArgs args) {
        return LettuceResult.toUni(_lpos(key, element, args));
    }

    Supplier<RedisFuture<Long>> _lpos(K key, V element, LPosArgs args) {
        nonNull(key, "key");
        nonNull(element, "element");
        io.lettuce.core.LPosArgs lettuceArgs = LettuceListCommandsConverters.toLettuceLPosArgs(args);
        return () -> async.lpos(marshaller.encode(key), marshaller.encode(element), lettuceArgs);
    }

    @Override
    public Uni<List<Long>> lpos(K key, V element, int count) {
        return LettuceResult.toUni(_lpos(key, element, count)).map(AbstractLettuceCommands::orEmpty);
    }

    Supplier<RedisFuture<List<Long>>> _lpos(K key, V element, int count) {
        nonNull(key, "key");
        nonNull(element, "element");
        positiveOrZero(count, "count"); // 0 -> All matches
        return () -> async.lpos(marshaller.encode(key), marshaller.encode(element), count);
    }

    @Override
    public Uni<List<Long>> lpos(K key, V element, int count, LPosArgs args) {
        return LettuceResult.toUni(_lpos(key, element, count, args)).map(AbstractLettuceCommands::orEmpty);
    }

    Supplier<RedisFuture<List<Long>>> _lpos(K key, V element, int count, LPosArgs args) {
        nonNull(key, "key");
        nonNull(element, "element");
        positiveOrZero(count, "count"); // 0 -> All matches
        io.lettuce.core.LPosArgs lettuceArgs = LettuceListCommandsConverters.toLettuceLPosArgs(args);
        return () -> async.lpos(marshaller.encode(key), marshaller.encode(element), count, lettuceArgs);
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
        return () -> async.lpush(marshaller.encode(key), marshaller.encodeAsArray(elements));
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
        return () -> async.lpushx(marshaller.encode(key), marshaller.encodeAsArray(elements));
    }

    @Override
    public Uni<List<V>> lrange(K key, long start, long stop) {
        return LettuceResult.toUni(_lrange(key, start, stop))
                .map(AbstractLettuceCommands::orEmpty)
                .map(this::decodeListOfValue);
    }

    Supplier<RedisFuture<List<byte[]>>> _lrange(K key, long start, long stop) {
        nonNull(key, "key");
        return () -> async.lrange(marshaller.encode(key), start, stop);
    }

    @Override
    public Uni<Long> lrem(K key, long count, V element) {
        return LettuceResult.toUni(_lrem(key, count, element));
    }

    Supplier<RedisFuture<Long>> _lrem(K key, long count, V element) {
        nonNull(key, "key");
        nonNull(element, "element");
        return () -> async.lrem(marshaller.encode(key), count, marshaller.encode(element));
    }

    @Override
    public Uni<Void> lset(K key, long index, V element) {
        return LettuceResult.toUni(_lset(key, index, element)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _lset(K key, long index, V element) {
        nonNull(key, "key");
        nonNull(element, "element");
        return () -> async.lset(marshaller.encode(key), index, marshaller.encode(element));
    }

    @Override
    public Uni<Void> ltrim(K key, long start, long stop) {
        return LettuceResult.toUni(_ltrim(key, start, stop)).replaceWithVoid();
    }

    Supplier<RedisFuture<String>> _ltrim(K key, long start, long stop) {
        nonNull(key, "key");
        return () -> async.ltrim(marshaller.encode(key), start, stop);
    }

    @Override
    public Uni<V> rpop(K key) {
        return LettuceResult.toUni(_rpop(key)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _rpop(K key) {
        nonNull(key, "key");
        return () -> async.rpop(marshaller.encode(key));
    }

    @Override
    public Uni<List<V>> rpop(K key, int count) {
        return LettuceResult.toUni(_rpop(key, count))
                .map(AbstractLettuceCommands::orEmpty)
                .map(this::decodeListOfValue);
    }

    Supplier<RedisFuture<List<byte[]>>> _rpop(K key, int count) {
        nonNull(key, "key");
        return () -> async.rpop(marshaller.encode(key), count);
    }

    @Deprecated
    @Override
    public Uni<V> rpoplpush(K source, K destination) {
        return LettuceResult.toUni(_rpoplpush(source, destination)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _rpoplpush(K source, K destination) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        return () -> async.rpoplpush(marshaller.encode(source), marshaller.encode(destination));
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
        return () -> async.rpush(marshaller.encode(key), marshaller.encodeAsArray(values));
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
        return () -> async.rpushx(marshaller.encode(key), marshaller.encodeAsArray(values));
    }

    @Override
    public Uni<List<V>> sort(K key) {
        return sort(key, DEFAULT_SORT_ARGS);
    }

    @Override
    public Uni<List<V>> sort(K key, SortArgs sortArguments) {
        return LettuceResult.toUni(_sort(key, sortArguments))
                .map(AbstractLettuceCommands::orEmpty)
                .map(this::decodeListOfValue);
    }

    Supplier<RedisFuture<List<byte[]>>> _sort(K key, SortArgs sortArguments) {
        nonNull(key, "key");
        nonNull(sortArguments, "sortArguments");
        io.lettuce.core.SortArgs lettuceArgs = LettuceListCommandsConverters.toLettuceSortArgs(sortArguments);
        return () -> async.sort(marshaller.encode(key), lettuceArgs);
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
        return () -> async.sortStore(marshaller.encode(key), lettuceArgs, marshaller.encode(destination));
    }

    @Override
    public Uni<Long> sortAndStore(K key, K destination) {
        return sortAndStore(key, destination, DEFAULT_SORT_ARGS);
    }

    KeyValue<K, V> toKeyValue(io.lettuce.core.KeyValue<byte[], byte[]> kv) {
        if (kv == null || kv.isEmpty()) {
            return null;
        }
        return KeyValue.of(decodeK(kv.getKey()), decodeV(kv.getValueOrElse(null)));
    }

    KeyValue<K, V> toFirstKeyValue(io.lettuce.core.KeyValue<byte[], List<byte[]>> kv) {
        if (kv == null || kv.isEmpty()) {
            return null;
        }
        List<byte[]> values = kv.getValue();
        if (values == null || values.isEmpty()) {
            return null;
        }
        return KeyValue.of(decodeK(kv.getKey()), decodeV(values.get(0)));
    }

    List<KeyValue<K, V>> toKeyValueList(io.lettuce.core.KeyValue<byte[], List<byte[]>> kv) {
        if (kv == null || kv.isEmpty()) {
            return Collections.emptyList();
        }
        List<byte[]> values = kv.getValue();
        if (values == null) {
            return Collections.emptyList();
        }
        K key = decodeK(kv.getKey());
        List<KeyValue<K, V>> result = new ArrayList<>(values.size());
        for (byte[] value : values) {
            result.add(KeyValue.of(key, decodeV(value)));
        }
        return result;
    }

}
