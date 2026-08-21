package io.quarkus.redis.runtime.client.lettuce.set;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.set.ReactiveSScanCursor;
import io.quarkus.redis.datasource.set.ReactiveSetCommands;
import io.quarkus.redis.runtime.client.lettuce.AbstractLettuceCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveSetCommands}, on top of
 * {@link RedisSetAsyncCommands} plus {@link RedisKeyAsyncCommands} for {@code SORT}.
 *
 * @param <K> the key type
 * @param <V> the member type
 */
public class LettuceReactiveSetCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveSetCommands<K, V> {

    private static final SortArgs DEFAULT_SORT_ARGS = new SortArgs();

    private final ReactiveRedisDataSource dataSource;

    private final RedisSetAsyncCommands<K, V> set = async;

    /**
     * {@code SORT} lives in Lettuce's key commands, not its set commands.
     */
    private final RedisKeyAsyncCommands<K, V> sortable = async;

    public LettuceReactiveSetCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<K, V> connection) {
        super(connection);
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
        // `members` is the name the Vert.x backend validates under, see the class Javadoc.
        notNullOrEmpty(values, "members");
        return () -> set.sadd(key, values);
    }

    @Override
    public Uni<Long> scard(K key) {
        return LettuceResult.toUni(_scard(key));
    }

    Supplier<RedisFuture<Long>> _scard(K key) {
        nonNull(key, "key");
        return () -> set.scard(key);
    }

    @SafeVarargs
    @Override
    public final Uni<Set<V>> sdiff(K... keys) {
        return LettuceResult.toUni(_sdiff(keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Set<V>>> _sdiff(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> set.sdiff(keys);
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
        return () -> set.sdiffstore(destination, keys);
    }

    @SafeVarargs
    @Override
    public final Uni<Set<V>> sinter(K... keys) {
        return LettuceResult.toUni(_sinter(keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Set<V>>> _sinter(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> set.sinter(keys);
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
        return () -> set.sintercard(keys);
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
        return () -> set.sintercard(limit, keys);
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
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> set.sinterstore(destination, keys);
    }

    @Override
    public Uni<Boolean> sismember(K key, V member) {
        return LettuceResult.toUni(_sismember(key, member));
    }

    Supplier<RedisFuture<Boolean>> _sismember(K key, V member) {
        nonNull(key, "key");
        nonNull(member, "member");
        return () -> set.sismember(key, member);
    }

    @Override
    public Uni<Set<V>> smembers(K key) {
        return LettuceResult.toUni(_smembers(key));
    }

    Supplier<RedisFuture<Set<V>>> _smembers(K key) {
        nonNull(key, "key");
        return () -> set.smembers(key);
    }

    @SafeVarargs
    @Override
    public final Uni<List<Boolean>> smismember(K key, V... members) {
        return LettuceResult.toUni(_smismember(key, members)).map(LettuceReactiveSetCommandsImpl::orEmpty);
    }

    @SafeVarargs
    final Supplier<RedisFuture<List<Boolean>>> _smismember(K key, V... members) {
        nonNull(key, "key");
        notNullOrEmpty(members, "members");
        return () -> set.smismember(key, members);
    }

    @Override
    public Uni<Boolean> smove(K source, K destination, V member) {
        return LettuceResult.toUni(_smove(source, destination, member));
    }

    Supplier<RedisFuture<Boolean>> _smove(K source, K destination, V member) {
        nonNull(source, "source");
        nonNull(destination, "destination");
        nonNull(member, "member");
        return () -> set.smove(source, destination, member);
    }

    @Override
    public Uni<V> spop(K key) {
        return LettuceResult.toUni(_spop(key));
    }

    Supplier<RedisFuture<V>> _spop(K key) {
        nonNull(key, "key");
        return () -> set.spop(key);
    }

    @Override
    public Uni<Set<V>> spop(K key, int count) {
        return LettuceResult.toUni(_spop(key, count));
    }

    Supplier<RedisFuture<Set<V>>> _spop(K key, int count) {
        nonNull(key, "key");
        positive(count, "count");
        return () -> set.spop(key, count);
    }

    @Override
    public Uni<V> srandmember(K key) {
        return LettuceResult.toUni(_srandmember(key));
    }

    Supplier<RedisFuture<V>> _srandmember(K key) {
        nonNull(key, "key");
        return () -> set.srandmember(key);
    }

    @Override
    public Uni<List<V>> srandmember(K key, int count) {
        return LettuceResult.toUni(_srandmember(key, count)).map(LettuceReactiveSetCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _srandmember(K key, int count) {
        nonNull(key, "key");
        return () -> set.srandmember(key, count);
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
        return () -> set.srem(key, members);
    }

    @SafeVarargs
    @Override
    public final Uni<Set<V>> sunion(K... keys) {
        return LettuceResult.toUni(_sunion(keys));
    }

    @SafeVarargs
    final Supplier<RedisFuture<Set<V>>> _sunion(K... keys) {
        notNullOrEmpty(keys, "keys");
        doesNotContainNull(keys, "keys");
        if (keys.length < 2) {
            return () -> {
                throw new IllegalArgumentException("`keys` must contain at least 2 keys");
            };
        }
        return () -> set.sunion(keys);
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
        return () -> set.sunionstore(destination, keys);
    }

    @Override
    public ReactiveSScanCursor<V> sscan(K key) {
        nonNull(key, "key");
        return new LettuceReactiveSScanCursorImpl<>(set, key);
    }

    @Override
    public ReactiveSScanCursor<V> sscan(K key, ScanArgs scanArgs) {
        nonNull(key, "key");
        nonNull(scanArgs, "scanArgs");
        return new LettuceReactiveSScanCursorImpl<>(set, key,
                LettuceSetCommandsConverters.toLettuceScanArgs(scanArgs));
    }

    @Override
    public Uni<List<V>> sort(K key) {
        return sort(key, DEFAULT_SORT_ARGS);
    }

    @Override
    public Uni<List<V>> sort(K key, SortArgs sortArguments) {
        return LettuceResult.toUni(_sort(key, sortArguments)).map(LettuceReactiveSetCommandsImpl::orEmpty);
    }

    Supplier<RedisFuture<List<V>>> _sort(K key, SortArgs sortArguments) {
        nonNull(key, "key");
        nonNull(sortArguments, "sortArguments");
        io.lettuce.core.SortArgs lettuceArgs = LettuceSetCommandsConverters.toLettuceSortArgs(sortArguments);
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
        io.lettuce.core.SortArgs lettuceArgs = LettuceSetCommandsConverters.toLettuceSortArgs(args);
        return () -> sortable.sortStore(key, lettuceArgs, destination);
    }

    @Override
    public Uni<Long> sortAndStore(K key, K destination) {
        return sortAndStore(key, destination, DEFAULT_SORT_ARGS);
    }
}
