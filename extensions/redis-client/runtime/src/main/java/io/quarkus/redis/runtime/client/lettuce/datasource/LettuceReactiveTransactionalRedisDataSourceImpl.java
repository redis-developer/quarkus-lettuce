package io.quarkus.redis.runtime.client.lettuce.datasource;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.quarkus.redis.datasource.autosuggest.ReactiveTransactionalAutoSuggestCommands;
import io.quarkus.redis.datasource.bitmap.ReactiveTransactionalBitMapCommands;
import io.quarkus.redis.datasource.bloom.ReactiveTransactionalBloomCommands;
import io.quarkus.redis.datasource.countmin.ReactiveTransactionalCountMinCommands;
import io.quarkus.redis.datasource.cuckoo.ReactiveTransactionalCuckooCommands;
import io.quarkus.redis.datasource.geo.ReactiveTransactionalGeoCommands;
import io.quarkus.redis.datasource.graph.ReactiveTransactionalGraphCommands;
import io.quarkus.redis.datasource.hash.ReactiveTransactionalHashCommands;
import io.quarkus.redis.datasource.hyperloglog.ReactiveTransactionalHyperLogLogCommands;
import io.quarkus.redis.datasource.json.ReactiveTransactionalJsonCommands;
import io.quarkus.redis.datasource.keys.ReactiveTransactionalKeyCommands;
import io.quarkus.redis.datasource.list.ReactiveTransactionalListCommands;
import io.quarkus.redis.datasource.search.ReactiveTransactionalSearchCommands;
import io.quarkus.redis.datasource.set.ReactiveTransactionalSetCommands;
import io.quarkus.redis.datasource.sortedset.ReactiveTransactionalSortedSetCommands;
import io.quarkus.redis.datasource.stream.ReactiveTransactionalStreamCommands;
import io.quarkus.redis.datasource.string.ReactiveTransactionalStringCommands;
import io.quarkus.redis.datasource.timeseries.ReactiveTransactionalTimeSeriesCommands;
import io.quarkus.redis.datasource.topk.ReactiveTransactionalTopKCommands;
import io.quarkus.redis.datasource.transactions.ReactiveTransactionalRedisDataSource;
import io.quarkus.redis.datasource.value.ReactiveTransactionalValueCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.quarkus.redis.runtime.client.lettuce.key.LettuceReactiveKeyCommandsImpl;
import io.quarkus.redis.runtime.client.lettuce.value.LettuceReactiveValueCommandsImpl;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Response;

/**
 * Lettuce-backed implementation of {@link ReactiveTransactionalRedisDataSource}.
 * <p>
 * Wraps the pinned {@link LettuceReactiveRedisDataSourceImpl} on which {@code MULTI} has already
 * been issued, together with the {@link LettuceTransactionHolder} that captures every queued
 * command. Command-group accessors return Lettuce transactional command groups that enqueue their
 * {@code RedisFuture}s on the same pinned connection; the orchestrator in
 * {@code LettuceReactiveRedisDataSourceImpl#withTransaction(...)} assembles the typed
 * {@code TransactionResult} once {@code EXEC} has run.
 * <p>
 * Only the {@code value}, {@code key} and {@code execute(...)} surfaces are wired — every other
 * command group throws {@link UnsupportedOperationException}, mirroring the shape of the
 * non-transactional Lettuce data source.
 */
public class LettuceReactiveTransactionalRedisDataSourceImpl implements ReactiveTransactionalRedisDataSource {

    private final LettuceReactiveRedisDataSourceImpl reactive;
    private final StatefulRedisConnection<String, String> connection;
    private final LettuceTransactionHolder tx;

    public LettuceReactiveTransactionalRedisDataSourceImpl(LettuceReactiveRedisDataSourceImpl reactive,
            LettuceTransactionHolder tx) {
        this.reactive = nonNull(reactive, "reactive");
        this.connection = reactive.getConnection();
        this.tx = nonNull(tx, "tx");
    }

    @Override
    public Uni<Void> discard() {
        return LettuceResult.toUni(() -> connection.async().discard())
                .invoke(tx::discard)
                .replaceWithVoid();
    }

    @Override
    public boolean discarded() {
        return tx.discarded();
    }

    @Override
    public <K, V> ReactiveTransactionalValueCommands<K, V> value(Class<K> redisKeyType, Class<V> valueType) {
        nonNull(redisKeyType, "redisKeyType");
        nonNull(valueType, "valueType");
        @SuppressWarnings("unchecked")
        LettuceReactiveValueCommandsImpl<K, V> reactiveValue = (LettuceReactiveValueCommandsImpl<K, V>) reactive
                .value(redisKeyType, valueType);
        return new LettuceReactiveTransactionalValueCommandsImpl<>(this, reactiveValue, tx);
    }

    @Override
    public <K, V> ReactiveTransactionalStringCommands<K, V> string(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("string");
    }

    @Override
    public <K> ReactiveTransactionalKeyCommands<K> key(Class<K> redisKeyType) {
        nonNull(redisKeyType, "redisKeyType");
        @SuppressWarnings("unchecked")
        LettuceReactiveKeyCommandsImpl<K, Object> reactiveKey = (LettuceReactiveKeyCommandsImpl<K, Object>) reactive
                .key(redisKeyType);
        return new LettuceReactiveTransactionalKeyCommandsImpl<>(this, reactiveKey, tx);
    }

    @Override
    public Uni<Void> execute(String command, String... args) {
        nonNull(command, "command");
        return enqueueRaw(LettuceReactiveRedisDataSourceImpl.resolve(command), args);
    }

    @Override
    public Uni<Void> execute(Command command, String... args) {
        nonNull(command, "command");
        return enqueueRaw(LettuceReactiveRedisDataSourceImpl.resolve(command.toString()), args);
    }

    @Override
    public Uni<Void> execute(io.vertx.redis.client.Command command, String... args) {
        nonNull(command, "command");
        return enqueueRaw(LettuceReactiveRedisDataSourceImpl.resolve(command.toString()), args);
    }

    @Override
    public <K, F, V> ReactiveTransactionalHashCommands<K, F, V> hash(Class<K> redisKeyType, Class<F> typeOfField,
            Class<V> typeOfValue) {
        throw groupNotImplemented("hash");
    }

    @Override
    public <K, V> ReactiveTransactionalGeoCommands<K, V> geo(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("geo");
    }

    @Override
    public <K, V> ReactiveTransactionalSortedSetCommands<K, V> sortedSet(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("sortedSet");
    }

    @Override
    public <K, V> ReactiveTransactionalSetCommands<K, V> set(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("set");
    }

    @Override
    public <K, V> ReactiveTransactionalListCommands<K, V> list(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("list");
    }

    @Override
    public <K, V> ReactiveTransactionalHyperLogLogCommands<K, V> hyperloglog(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("hyperloglog");
    }

    @Override
    public <K> ReactiveTransactionalBitMapCommands<K> bitmap(Class<K> redisKeyType) {
        throw groupNotImplemented("bitmap");
    }

    @Override
    public <K, F, V> ReactiveTransactionalStreamCommands<K, F, V> stream(Class<K> redisKeyType, Class<F> typeOfField,
            Class<V> typeOfValue) {
        throw groupNotImplemented("stream");
    }

    @Override
    public <K> ReactiveTransactionalJsonCommands<K> json(Class<K> redisKeyType) {
        throw groupNotImplemented("json");
    }

    @Override
    public <K, V> ReactiveTransactionalBloomCommands<K, V> bloom(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("bloom");
    }

    @Override
    public <K, V> ReactiveTransactionalCuckooCommands<K, V> cuckoo(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("cuckoo");
    }

    @Override
    public <K, V> ReactiveTransactionalCountMinCommands<K, V> countmin(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("countmin");
    }

    @Override
    public <K, V> ReactiveTransactionalTopKCommands<K, V> topk(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("topk");
    }

    @Override
    public <K> ReactiveTransactionalGraphCommands<K> graph(Class<K> redisKeyType) {
        throw groupNotImplemented("graph");
    }

    @Override
    public <K> ReactiveTransactionalSearchCommands search(Class<K> redisKeyType) {
        throw groupNotImplemented("search");
    }

    @Override
    public <K> ReactiveTransactionalAutoSuggestCommands<K> autosuggest(Class<K> redisKeyType) {
        throw groupNotImplemented("autosuggest");
    }

    @Override
    public <K> ReactiveTransactionalTimeSeriesCommands<K> timeseries(Class<K> redisKeyType) {
        throw groupNotImplemented("timeseries");
    }

    private Uni<Void> enqueueRaw(ProtocolKeyword type, String... args) {
        LettuceVertxResponseOutput<String, String> output = new LettuceVertxResponseOutput<>(StringCodec.UTF8);
        CommandArgs<String, String> commandArgs = new CommandArgs<>(StringCodec.UTF8);
        if (args != null) {
            for (String arg : args) {
                if (arg != null) {
                    commandArgs.add(arg);
                }
            }
        }
        return tx.enqueue(() -> connection.async().dispatch(type, output, commandArgs), ignored -> {
            io.vertx.redis.client.Response raw = output.toVertxResponse();
            return raw == null ? null : Response.newInstance(raw);
        });
    }

    private static UnsupportedOperationException groupNotImplemented(String group) {
        return new UnsupportedOperationException(
                "The '" + group + "' transactional command group is not yet implemented on the Lettuce backend. "
                        + "Set quarkus.redis.backend=vertx to use the Vert.x backend.");
    }
}
