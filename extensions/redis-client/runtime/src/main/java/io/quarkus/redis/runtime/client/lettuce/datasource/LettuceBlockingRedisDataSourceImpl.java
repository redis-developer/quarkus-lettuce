package io.quarkus.redis.runtime.client.lettuce.datasource;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fasterxml.jackson.core.type.TypeReference;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.autosuggest.AutoSuggestCommands;
import io.quarkus.redis.datasource.bitmap.BitMapCommands;
import io.quarkus.redis.datasource.bloom.BloomCommands;
import io.quarkus.redis.datasource.countmin.CountMinCommands;
import io.quarkus.redis.datasource.cuckoo.CuckooCommands;
import io.quarkus.redis.datasource.geo.GeoCommands;
import io.quarkus.redis.datasource.graph.GraphCommands;
import io.quarkus.redis.datasource.hash.HashCommands;
import io.quarkus.redis.datasource.hyperloglog.HyperLogLogCommands;
import io.quarkus.redis.datasource.json.JsonCommands;
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.list.ListCommands;
import io.quarkus.redis.datasource.pubsub.PubSubCommands;
import io.quarkus.redis.datasource.search.SearchCommands;
import io.quarkus.redis.datasource.set.SetCommands;
import io.quarkus.redis.datasource.sortedset.SortedSetCommands;
import io.quarkus.redis.datasource.stream.StreamCommands;
import io.quarkus.redis.datasource.string.StringCommands;
import io.quarkus.redis.datasource.timeseries.TimeSeriesCommands;
import io.quarkus.redis.datasource.topk.TopKCommands;
import io.quarkus.redis.datasource.transactions.OptimisticLockingTransactionResult;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.datasource.transactions.TransactionalRedisDataSource;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.value.ValueCommands;
import io.quarkus.redis.runtime.client.lettuce.key.LettuceBlockingKeyCommandsImpl;
import io.quarkus.redis.runtime.client.lettuce.value.LettuceBlockingValueCommandsImpl;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Response;

/**
 * Blocking wrapper around {@link LettuceReactiveRedisDataSourceImpl}.
 * <p>
 * Each method awaits the reactive result for the configured {@code timeout}. Must not be
 * invoked from an event loop thread. {@code withConnection(...)} runs the user block on a
 * dedicated connection borrowed from the underlying reactive data source.
 */
public class LettuceBlockingRedisDataSourceImpl implements RedisDataSource {

    private final LettuceReactiveRedisDataSourceImpl reactive;
    private final Duration timeout;
    private final boolean pinned;

    public LettuceBlockingRedisDataSourceImpl(LettuceReactiveRedisDataSourceImpl reactive, Duration timeout) {
        this(reactive, timeout, false);
    }

    private LettuceBlockingRedisDataSourceImpl(LettuceReactiveRedisDataSourceImpl reactive, Duration timeout, boolean pinned) {
        this.reactive = nonNull(reactive, "reactive");
        this.timeout = nonNull(timeout, "timeout");
        this.pinned = pinned;
    }

    static LettuceBlockingRedisDataSourceImpl pinnedTo(LettuceReactiveRedisDataSourceImpl pinnedReactive, Duration timeout) {
        return new LettuceBlockingRedisDataSourceImpl(pinnedReactive, timeout, true);
    }

    @Override
    public ReactiveRedisDataSource getReactive() {
        return reactive;
    }

    @Override
    public Response execute(String command, String... args) {
        return reactive.execute(command, args).await().atMost(timeout);
    }

    @Override
    public Response execute(Command command, String... args) {
        return reactive.execute(command, args).await().atMost(timeout);
    }

    @Override
    public Response execute(io.vertx.redis.client.Command command, String... args) {
        return reactive.execute(command, args).await().atMost(timeout);
    }

    @Override
    public void flushall() {
        reactive.flushall().await().atMost(timeout);
    }

    @Override
    public void select(long index) {
        reactive.select(index).await().atMost(timeout);
    }

    @Override
    public void withConnection(Consumer<RedisDataSource> consumer) {
        if (pinned) {
            consumer.accept(this);
            return;
        }
        reactive.withConnection(rds -> {
            LettuceReactiveRedisDataSourceImpl pinnedReactive = (LettuceReactiveRedisDataSourceImpl) rds;
            LettuceBlockingRedisDataSourceImpl pinnedBlocking = pinnedTo(pinnedReactive, timeout);
            consumer.accept(pinnedBlocking);
            return Uni.createFrom().voidItem();
        }).await().atMost(timeout);
    }

    @Override
    public TransactionResult withTransaction(Consumer<TransactionalRedisDataSource> tx) {
        throw transactionsNotSupported();
    }

    @Override
    public TransactionResult withTransaction(Consumer<TransactionalRedisDataSource> tx, String... watchedKeys) {
        throw transactionsNotSupported();
    }

    @Override
    public <I> OptimisticLockingTransactionResult<I> withTransaction(Function<RedisDataSource, I> preTx,
            BiConsumer<I, TransactionalRedisDataSource> tx, String... watchedKeys) {
        throw transactionsNotSupported();
    }

    private static UnsupportedOperationException transactionsNotSupported() {
        return new UnsupportedOperationException(
                "Transactions and dedicated connections are not yet supported on the Lettuce backend. "
                        + "Set quarkus.redis.backend=vertx to use the Vert.x backend.");
    }

    @Override
    public <K, V> ValueCommands<K, V> value(Class<K> redisKeyType, Class<V> valueType) {
        ReactiveValueCommands<K, V> r = reactive.value(redisKeyType, valueType);
        return new LettuceBlockingValueCommandsImpl<>(this, r, timeout);
    }

    @Override
    public <K, V> ValueCommands<K, V> value(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("value(TypeReference, TypeReference)");
    }

    @Override
    public <K, V> StringCommands<K, V> string(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("string");
    }

    @Override
    public <K, F, V> HashCommands<K, F, V> hash(Class<K> redisKeyType, Class<F> typeOfField, Class<V> typeOfValue) {
        throw groupNotImplemented("hash");
    }

    @Override
    public <K, F, V> HashCommands<K, F, V> hash(TypeReference<K> redisKeyType, TypeReference<F> typeOfField,
            TypeReference<V> typeOfValue) {
        throw groupNotImplemented("hash");
    }

    @Override
    public <K, V> GeoCommands<K, V> geo(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("geo");
    }

    @Override
    public <K, V> GeoCommands<K, V> geo(TypeReference<K> redisKeyType, TypeReference<V> memberType) {
        throw groupNotImplemented("geo");
    }

    @Override
    public <K> KeyCommands<K> key(Class<K> redisKeyType) {
        return new LettuceBlockingKeyCommandsImpl<>(this, reactive.key(redisKeyType), timeout);
    }

    @Override
    public <K> KeyCommands<K> key(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("key(TypeReference)");
    }

    @Override
    public <K, V> SortedSetCommands<K, V> sortedSet(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("sortedSet");
    }

    @Override
    public <K, V> SortedSetCommands<K, V> sortedSet(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("sortedSet");
    }

    @Override
    public <K, V> SetCommands<K, V> set(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("set");
    }

    @Override
    public <K, V> SetCommands<K, V> set(TypeReference<K> redisKeyType, TypeReference<V> memberType) {
        throw groupNotImplemented("set");
    }

    @Override
    public <K, V> ListCommands<K, V> list(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("list");
    }

    @Override
    public <K, V> ListCommands<K, V> list(TypeReference<K> redisKeyType, TypeReference<V> memberType) {
        throw groupNotImplemented("list");
    }

    @Override
    public <K, V> HyperLogLogCommands<K, V> hyperloglog(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("hyperloglog");
    }

    @Override
    public <K, V> HyperLogLogCommands<K, V> hyperloglog(TypeReference<K> redisKeyType, TypeReference<V> memberType) {
        throw groupNotImplemented("hyperloglog");
    }

    @Override
    public <K> BitMapCommands<K> bitmap(Class<K> redisKeyType) {
        throw groupNotImplemented("bitmap");
    }

    @Override
    public <K> BitMapCommands<K> bitmap(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("bitmap");
    }

    @Override
    public <K, F, V> StreamCommands<K, F, V> stream(Class<K> redisKeyType, Class<F> fieldType, Class<V> valueType) {
        throw groupNotImplemented("stream");
    }

    @Override
    public <K, F, V> StreamCommands<K, F, V> stream(TypeReference<K> redisKeyType, TypeReference<F> fieldType,
            TypeReference<V> valueType) {
        throw groupNotImplemented("stream");
    }

    @Override
    public <V> PubSubCommands<V> pubsub(Class<V> messageType) {
        throw groupNotImplemented("pubsub");
    }

    @Override
    public <V> PubSubCommands<V> pubsub(TypeReference<V> messageType) {
        throw groupNotImplemented("pubsub");
    }

    @Override
    public <K> JsonCommands<K> json(Class<K> redisKeyType) {
        throw groupNotImplemented("json");
    }

    @Override
    public <K> JsonCommands<K> json(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("json");
    }

    @Override
    public <K, V> BloomCommands<K, V> bloom(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("bloom");
    }

    @Override
    public <K, V> BloomCommands<K, V> bloom(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("bloom");
    }

    @Override
    public <K, V> CuckooCommands<K, V> cuckoo(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("cuckoo");
    }

    @Override
    public <K, V> CuckooCommands<K, V> cuckoo(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("cuckoo");
    }

    @Override
    public <K, V> CountMinCommands<K, V> countmin(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("countmin");
    }

    @Override
    public <K, V> CountMinCommands<K, V> countmin(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("countmin");
    }

    @Override
    public <K, V> TopKCommands<K, V> topk(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("topk");
    }

    @Override
    public <K, V> TopKCommands<K, V> topk(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("topk");
    }

    @Override
    public <K> GraphCommands<K> graph(Class<K> redisKeyType) {
        throw groupNotImplemented("graph");
    }

    @Override
    public <K> SearchCommands<K> search(Class<K> redisKeyType) {
        throw groupNotImplemented("search");
    }

    @Override
    public <K> AutoSuggestCommands<K> autosuggest(Class<K> redisKeyType) {
        throw groupNotImplemented("autosuggest");
    }

    @Override
    public <K> AutoSuggestCommands<K> autosuggest(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("autosuggest");
    }

    @Override
    public <K> TimeSeriesCommands<K> timeseries(Class<K> redisKeyType) {
        throw groupNotImplemented("timeseries");
    }

    @Override
    public <K> TimeSeriesCommands<K> timeseries(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("timeseries");
    }

    private static UnsupportedOperationException groupNotImplemented(String group) {
        return new UnsupportedOperationException(
                "The '" + group + "' command group is not yet implemented on the Lettuce backend. "
                        + "Set quarkus.redis.backend=vertx to use the Vert.x backend.");
    }
}
