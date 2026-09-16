package io.quarkus.redis.lettuce.runtime.internal.set;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.lang.reflect.Type;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.set.ReactiveSScanCursor;
import io.quarkus.redis.datasource.set.ReactiveSetCommands;
import io.quarkus.redis.lettuce.runtime.internal.AbstractLettuceCommands;
import io.quarkus.redis.lettuce.runtime.internal.LettuceResult;
import io.quarkus.redis.runtime.datasource.Marshaller;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveSetCommands}.
 *
 * @param <K> the key type
 * @param <V> the member type
 */
public class LettuceReactiveSetCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveSetCommands<K, V> {

    private static final SortArgs DEFAULT_SORT_ARGS = new SortArgs();

    private final ReactiveRedisDataSource dataSource;

    public LettuceReactiveSetCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<byte[], byte[]> connection, Type keyType, Type valueType) {
        super(connection, keyType, valueType, new Marshaller(keyType, valueType));
        this.dataSource = dataSource;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> sadd(K key, V... values) {
        return LettuceResult.toUni(_sadd(key, values)).map(Long::intValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _sadd(K key, V... values) {
        nonNull(key, "key");
        notNullOrEmpty(values, "members");
        doesNotContainNull(values, "members");
        return () -> async.sadd(marshaller.encode(key), marshaller.encodeAsArray(values));
    }

    @Override
    public Uni<Long> scard(K key) {
        return LettuceResult.toUni(_scard(key));
    }

    Supplier<RedisFuture<Long>> _scard(K key) {
        nonNull(key, "key");
        return () -> async.scard(marshaller.encode(key));
    }

    @SafeVarargs
    @Override
    public final Uni<Set<V>> sdiff(K... keys) {
        return LettuceResult.toUni(_sdiff(keys)).map(this::decodeSetOfValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Set<byte[]>>> _sdiff(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> async.sdiff(marshaller.encodeAsArray(keys));
    }

    @SafeVarargs
    @Override
    public final Uni<Long> sdiffstore(K destination, K... keys) {
        return LettuceResult.toUni(_sdiffstore(destination, keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _sdiffstore(K destination, K... keys) {
        nonNull(destination, "destination");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> async.sdiffstore(marshaller.encode(destination), marshaller.encodeAsArray(keys));
    }

    @SafeVarargs
    @Override
    public final Uni<Set<V>> sinter(K... keys) {
        return LettuceResult.toUni(_sinter(keys)).map(this::decodeSetOfValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Set<byte[]>>> _sinter(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> async.sinter(marshaller.encodeAsArray(keys));
    }

    @SafeVarargs
    @Override
    public final Uni<Long> sintercard(K... keys) {
        return LettuceResult.toUni(_sintercard(keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _sintercard(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> async.sintercard(marshaller.encodeAsArray(keys));
    }

    @SafeVarargs
    @Override
    public final Uni<Long> sintercard(int limit, K... keys) {
        return LettuceResult.toUni(_sintercard(limit, keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _sintercard(int limit, K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        positive(limit, "limit");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> async.sintercard(limit, marshaller.encodeAsArray(keys));
    }

    @SafeVarargs
    @Override
    public final Uni<Long> sinterstore(K destination, K... keys) {
        return LettuceResult.toUni(_sinterstore(destination, keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _sinterstore(K destination, K... keys) {
        nonNull(destination, "destination");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> async.sinterstore(marshaller.encode(destination), marshaller.encodeAsArray(keys));
    }

    @Override
    public Uni<Boolean> sismember(K key, V member) {
        return LettuceResult.toUni(_sismember(key, member));
    }

    Supplier<RedisFuture<Boolean>> _sismember(K key, V member) {
        nonNull(key, "key");
        nonNull(member, "member");
        return () -> async.sismember(marshaller.encode(key), marshaller.encode(member));
    }

    @Override
    public Uni<Set<V>> smembers(K key) {
        return LettuceResult.toUni(_smembers(key)).map(this::decodeSetOfValue);
    }

    Supplier<RedisFuture<Set<byte[]>>> _smembers(K key) {
        nonNull(key, "key");
        return () -> async.smembers(marshaller.encode(key));
    }

    @SafeVarargs
    @Override
    public final Uni<List<Boolean>> smismember(K key, V... members) {
        return LettuceResult.toUni(_smismember(key, members)).map(AbstractLettuceCommands::orEmpty);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<Boolean>>> _smismember(K key, V... members) {
        nonNull(key, "key");
        notNullOrEmpty(members, "members");
        doesNotContainNull(members, "members");
        return () -> async.smismember(marshaller.encode(key), marshaller.encodeAsArray(members));
    }

    @Override
    public Uni<Boolean> smove(K source, K destination, V member) {
        return LettuceResult.toUni(_smove(source, destination, member));
    }

    Supplier<RedisFuture<Boolean>> _smove(K source, K destination, V member) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        nonNull(member, "member");
        return () -> async.smove(marshaller.encode(source), marshaller.encode(destination), marshaller.encode(member));
    }

    @Override
    public Uni<V> spop(K key) {
        return LettuceResult.toUni(_spop(key)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _spop(K key) {
        nonNull(key, "key");
        return () -> async.spop(marshaller.encode(key));
    }

    @Override
    public Uni<Set<V>> spop(K key, int count) {
        return LettuceResult.toUni(_spop(key, count)).map(this::decodeSetOfValue);
    }

    Supplier<RedisFuture<Set<byte[]>>> _spop(K key, int count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> async.spop(marshaller.encode(key), count);
    }

    @Override
    public Uni<V> srandmember(K key) {
        return LettuceResult.toUni(_srandmember(key)).map(this::decodeV);
    }

    Supplier<RedisFuture<byte[]>> _srandmember(K key) {
        nonNull(key, "key");
        return () -> async.srandmember(marshaller.encode(key));
    }

    @Override
    public Uni<List<V>> srandmember(K key, int count) {
        return LettuceResult.toUni(_srandmember(key, count))
                .map(AbstractLettuceCommands::orEmpty)
                .map(this::decodeListOfValue);
    }

    Supplier<RedisFuture<List<byte[]>>> _srandmember(K key, int count) {
        nonNull(key, "key");
        return () -> async.srandmember(marshaller.encode(key), count);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> srem(K key, V... members) {
        return LettuceResult.toUni(_srem(key, members)).map(Long::intValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _srem(K key, V... members) {
        nonNull(key, "key");
        notNullOrEmpty(members, "members");
        doesNotContainNull(members, "members");
        return () -> async.srem(marshaller.encode(key), marshaller.encodeAsArray(members));
    }

    @SafeVarargs
    @Override
    public final Uni<Set<V>> sunion(K... keys) {
        return LettuceResult.toUni(_sunion(keys)).map(this::decodeSetOfValue);
    }

    @SafeVarargs
    final Supplier<RedisFuture<Set<byte[]>>> _sunion(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> async.sunion(marshaller.encodeAsArray(keys));
    }

    @SafeVarargs
    @Override
    public final Uni<Long> sunionstore(K destination, K... keys) {
        return LettuceResult.toUni(_sunionstore(destination, keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Long>> _sunionstore(K destination, K... keys) {
        nonNull(destination, "destination");
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> async.sunionstore(marshaller.encode(destination), marshaller.encodeAsArray(keys));
    }

    @Override
    public ReactiveSScanCursor<V> sscan(K key) {
        nonNull(key, "key");
        return new LettuceReactiveSScanCursorImpl<>(async, marshaller.encode(key), this::decodeListOfValue);
    }

    @Override
    public ReactiveSScanCursor<V> sscan(K key, ScanArgs scanArgs) {
        nonNull(key, "key");
        nonNull(scanArgs, "scanArgs");
        return new LettuceReactiveSScanCursorImpl<>(async, marshaller.encode(key),
                LettuceSetCommandsConverters.toLettuceScanArgs(scanArgs), this::decodeListOfValue);
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
        io.lettuce.core.SortArgs lettuceArgs = LettuceSetCommandsConverters.toLettuceSortArgs(sortArguments);
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
        io.lettuce.core.SortArgs lettuceArgs = LettuceSetCommandsConverters.toLettuceSortArgs(args);
        return () -> async.sortStore(marshaller.encode(key), lettuceArgs, marshaller.encode(destination));
    }

    @Override
    public Uni<Long> sortAndStore(K key, K destination) {
        return sortAndStore(key, destination, DEFAULT_SORT_ARGS);
    }

    Set<V> decodeSetOfValue(Set<byte[]> members) {
        Set<V> decoded = new LinkedHashSet<>(members.size());
        for (byte[] member : members) {
            decoded.add(decodeV(member));
        }
        return decoded;
    }

}
