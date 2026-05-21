package io.quarkus.redis.runtime.client.lettuce.datasource;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positiveOrZero;

import java.util.function.BiFunction;
import java.util.function.Function;

import com.fasterxml.jackson.core.type.TypeReference;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.autosuggest.ReactiveAutoSuggestCommands;
import io.quarkus.redis.datasource.bitmap.ReactiveBitMapCommands;
import io.quarkus.redis.datasource.bloom.ReactiveBloomCommands;
import io.quarkus.redis.datasource.countmin.ReactiveCountMinCommands;
import io.quarkus.redis.datasource.cuckoo.ReactiveCuckooCommands;
import io.quarkus.redis.datasource.geo.ReactiveGeoCommands;
import io.quarkus.redis.datasource.graph.ReactiveGraphCommands;
import io.quarkus.redis.datasource.hash.ReactiveHashCommands;
import io.quarkus.redis.datasource.hyperloglog.ReactiveHyperLogLogCommands;
import io.quarkus.redis.datasource.json.ReactiveJsonCommands;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.list.ReactiveListCommands;
import io.quarkus.redis.datasource.pubsub.ReactivePubSubCommands;
import io.quarkus.redis.datasource.search.ReactiveSearchCommands;
import io.quarkus.redis.datasource.set.ReactiveSetCommands;
import io.quarkus.redis.datasource.sortedset.ReactiveSortedSetCommands;
import io.quarkus.redis.datasource.stream.ReactiveStreamCommands;
import io.quarkus.redis.datasource.string.ReactiveStringCommands;
import io.quarkus.redis.datasource.timeseries.ReactiveTimeSeriesCommands;
import io.quarkus.redis.datasource.topk.ReactiveTopKCommands;
import io.quarkus.redis.datasource.transactions.OptimisticLockingTransactionResult;
import io.quarkus.redis.datasource.transactions.ReactiveTransactionalRedisDataSource;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.quarkus.redis.runtime.client.lettuce.key.LettuceReactiveKeyCommandsImpl;
import io.quarkus.redis.runtime.client.lettuce.value.LettuceReactiveValueCommandsImpl;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.Response;

/**
 * Lettuce-backed implementation of {@link ReactiveRedisDataSource}.
 * <p>
 * Wires the {@code value} command group to {@link LettuceReactiveValueCommandsImpl} and
 * implements {@code execute(...)}, {@code flushall()} and {@code select(...)} on top of the
 * Lettuce async API. {@code getRedis()} and the {@code withConnection}/{@code withTransaction}
 * blocks throw {@link UnsupportedOperationException} for now.
 */
public class LettuceReactiveRedisDataSourceImpl implements ReactiveRedisDataSource {

    private final Vertx vertx;
    private final StatefulRedisConnection<String, String> connection;

    public LettuceReactiveRedisDataSourceImpl(Vertx vertx, StatefulRedisConnection<String, String> connection) {
        this.vertx = nonNull(vertx, "vertx");
        this.connection = nonNull(connection, "connection");
    }

    public Vertx getVertx() {
        return vertx;
    }

    public StatefulRedisConnection<String, String> getConnection() {
        return connection;
    }

    @Override
    public Uni<Response> execute(String command, String... args) {
        nonNull(command, "command");
        return dispatch(resolve(command), args);
    }

    @Override
    public Uni<Response> execute(Command command, String... args) {
        nonNull(command, "command");
        return dispatch(resolve(command.toString()), args);
    }

    @Override
    public Uni<Response> execute(io.vertx.redis.client.Command command, String... args) {
        nonNull(command, "command");
        return dispatch(resolve(command.toString()), args);
    }

    private Uni<Response> dispatch(ProtocolKeyword type, String... args) {
        LettuceVertxResponseOutput<String, String> output = new LettuceVertxResponseOutput<>(StringCodec.UTF8);
        CommandArgs<String, String> commandArgs = new CommandArgs<>(StringCodec.UTF8);
        if (args != null) {
            for (String arg : args) {
                if (arg != null) {
                    commandArgs.add(arg);
                }
            }
        }
        return LettuceResult.toUni(() -> connection.async().dispatch(type, output, commandArgs))
                .map(ignored -> {
                    io.vertx.redis.client.Response raw = output.toVertxResponse();
                    return raw == null ? null : Response.newInstance(raw);
                });
    }

    private static ProtocolKeyword resolve(String name) {
        try {
            return CommandType.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return new StringKeyword(name);
        }
    }

    @Override
    public Uni<Void> flushall() {
        return LettuceResult.toUni(() -> connection.async().flushall()).replaceWithVoid();
    }

    @Override
    public Uni<Void> select(long index) {
        positiveOrZero(index, "index");
        return LettuceResult.toUni(() -> connection.async().select((int) index)).replaceWithVoid();
    }

    @Override
    public Redis getRedis() {
        throw new UnsupportedOperationException(
                "getRedis() is not supported on the Lettuce backend: it returns a Vert.x-specific type "
                        + "(io.vertx.mutiny.redis.client.Redis) that has no Lettuce equivalent. "
                        + "Either use execute(command, args...) for raw Redis commands, "
                        + "or set quarkus.redis.backend=vertx to use the Vert.x backend.");
    }

    @Override
    public Uni<Void> withConnection(Function<ReactiveRedisDataSource, Uni<Void>> function) {
        throw transactionsNotSupported();
    }

    @Override
    public Uni<TransactionResult> withTransaction(Function<ReactiveTransactionalRedisDataSource, Uni<Void>> tx) {
        throw transactionsNotSupported();
    }

    @Override
    public Uni<TransactionResult> withTransaction(Function<ReactiveTransactionalRedisDataSource, Uni<Void>> tx,
            String... watchedKeys) {
        throw transactionsNotSupported();
    }

    @Override
    public <I> Uni<OptimisticLockingTransactionResult<I>> withTransaction(
            Function<ReactiveRedisDataSource, Uni<I>> preTx,
            BiFunction<I, ReactiveTransactionalRedisDataSource, Uni<Void>> tx,
            String... watchedKeys) {
        throw transactionsNotSupported();
    }

    private static UnsupportedOperationException transactionsNotSupported() {
        return new UnsupportedOperationException(
                "Transactions and dedicated connections are not yet supported on the Lettuce backend. "
                        + "Set quarkus.redis.backend=vertx to use the Vert.x backend.");
    }

    @Override
    public <K, V> ReactiveValueCommands<K, V> value(Class<K> redisKeyType, Class<V> valueType) {
        nonNull(redisKeyType, "redisKeyType");
        nonNull(valueType, "valueType");
        @SuppressWarnings("unchecked")
        StatefulRedisConnection<K, V> typedConnection = (StatefulRedisConnection<K, V>) connection;
        return new LettuceReactiveValueCommandsImpl<>(this, typedConnection);
    }

    @Override
    public <K, V> ReactiveValueCommands<K, V> value(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("value(TypeReference, TypeReference)");
    }

    @Override
    public <K, V> ReactiveStringCommands<K, V> string(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("string");
    }

    @Override
    public <K, F, V> ReactiveHashCommands<K, F, V> hash(Class<K> redisKeyType, Class<F> fieldType, Class<V> valueType) {
        throw groupNotImplemented("hash");
    }

    @Override
    public <K, F, V> ReactiveHashCommands<K, F, V> hash(TypeReference<K> redisKeyType, TypeReference<F> fieldType,
            TypeReference<V> valueType) {
        throw groupNotImplemented("hash");
    }

    @Override
    public <K, V> ReactiveGeoCommands<K, V> geo(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("geo");
    }

    @Override
    public <K, V> ReactiveGeoCommands<K, V> geo(TypeReference<K> redisKeyType, TypeReference<V> memberType) {
        throw groupNotImplemented("geo");
    }

    @Override
    public <K> ReactiveKeyCommands<K> key(Class<K> redisKeyType) {
        nonNull(redisKeyType, "redisKeyType");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        StatefulRedisConnection<K, Object> typedConnection = (StatefulRedisConnection) connection;
        return new LettuceReactiveKeyCommandsImpl<>(this, typedConnection);
    }

    @Override
    public <K> ReactiveKeyCommands<K> key(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("key(TypeReference)");
    }

    @Override
    public <K, V> ReactiveSortedSetCommands<K, V> sortedSet(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("sortedSet");
    }

    @Override
    public <K, V> ReactiveSortedSetCommands<K, V> sortedSet(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("sortedSet");
    }

    @Override
    public <K, V> ReactiveSetCommands<K, V> set(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("set");
    }

    @Override
    public <K, V> ReactiveSetCommands<K, V> set(TypeReference<K> redisKeyType, TypeReference<V> memberType) {
        throw groupNotImplemented("set");
    }

    @Override
    public <K, V> ReactiveListCommands<K, V> list(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("list");
    }

    @Override
    public <K, V> ReactiveListCommands<K, V> list(TypeReference<K> redisKeyType, TypeReference<V> memberType) {
        throw groupNotImplemented("list");
    }

    @Override
    public <K, V> ReactiveHyperLogLogCommands<K, V> hyperloglog(Class<K> redisKeyType, Class<V> memberType) {
        throw groupNotImplemented("hyperloglog");
    }

    @Override
    public <K, V> ReactiveHyperLogLogCommands<K, V> hyperloglog(TypeReference<K> redisKeyType, TypeReference<V> memberType) {
        throw groupNotImplemented("hyperloglog");
    }

    @Override
    public <K> ReactiveBitMapCommands<K> bitmap(Class<K> redisKeyType) {
        throw groupNotImplemented("bitmap");
    }

    @Override
    public <K> ReactiveBitMapCommands<K> bitmap(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("bitmap");
    }

    @Override
    public <K, F, V> ReactiveStreamCommands<K, F, V> stream(Class<K> redisKeyType, Class<F> fieldType, Class<V> valueType) {
        throw groupNotImplemented("stream");
    }

    @Override
    public <K, F, V> ReactiveStreamCommands<K, F, V> stream(TypeReference<K> redisKeyType, TypeReference<F> fieldType,
            TypeReference<V> valueType) {
        throw groupNotImplemented("stream");
    }

    @Override
    public <V> ReactivePubSubCommands<V> pubsub(Class<V> messageType) {
        throw groupNotImplemented("pubsub");
    }

    @Override
    public <V> ReactivePubSubCommands<V> pubsub(TypeReference<V> messageType) {
        throw groupNotImplemented("pubsub");
    }

    @Override
    public <K> ReactiveJsonCommands<K> json(Class<K> redisKeyType) {
        throw groupNotImplemented("json");
    }

    @Override
    public <K> ReactiveJsonCommands<K> json(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("json");
    }

    @Override
    public <K, V> ReactiveBloomCommands<K, V> bloom(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("bloom");
    }

    @Override
    public <K, V> ReactiveBloomCommands<K, V> bloom(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("bloom");
    }

    @Override
    public <K, V> ReactiveCuckooCommands<K, V> cuckoo(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("cuckoo");
    }

    @Override
    public <K, V> ReactiveCuckooCommands<K, V> cuckoo(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("cuckoo");
    }

    @Override
    public <K, V> ReactiveCountMinCommands<K, V> countmin(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("countmin");
    }

    @Override
    public <K, V> ReactiveCountMinCommands<K, V> countmin(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("countmin");
    }

    @Override
    public <K, V> ReactiveTopKCommands<K, V> topk(Class<K> redisKeyType, Class<V> valueType) {
        throw groupNotImplemented("topk");
    }

    @Override
    public <K, V> ReactiveTopKCommands<K, V> topk(TypeReference<K> redisKeyType, TypeReference<V> valueType) {
        throw groupNotImplemented("topk");
    }

    @Override
    public <K> ReactiveGraphCommands<K> graph(Class<K> redisKeyType) {
        throw groupNotImplemented("graph");
    }

    @Override
    public <K> ReactiveSearchCommands<K> search(Class<K> redisKeyType) {
        throw groupNotImplemented("search");
    }

    @Override
    public <K> ReactiveAutoSuggestCommands<K> autosuggest(Class<K> redisKeyType) {
        throw groupNotImplemented("autosuggest");
    }

    @Override
    public <K> ReactiveAutoSuggestCommands<K> autosuggest(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("autosuggest");
    }

    @Override
    public <K> ReactiveTimeSeriesCommands<K> timeseries(Class<K> redisKeyType) {
        throw groupNotImplemented("timeseries");
    }

    @Override
    public <K> ReactiveTimeSeriesCommands<K> timeseries(TypeReference<K> redisKeyType) {
        throw groupNotImplemented("timeseries");
    }

    private static UnsupportedOperationException groupNotImplemented(String group) {
        return new UnsupportedOperationException(
                "The '" + group + "' command group is not yet implemented on the Lettuce backend. "
                        + "Set quarkus.redis.backend=vertx to use the Vert.x backend.");
    }

    /**
     * Fallback {@link ProtocolKeyword} for raw command names not present in
     * {@link CommandType}. Allows {@code execute("DEBUG", "SLEEP", "0")} and similar
     * extension/module commands to be dispatched without requiring an enum entry.
     */
    private static final class StringKeyword implements ProtocolKeyword {

        private final String name;
        private final byte[] bytes;

        StringKeyword(String name) {
            this.name = name;
            this.bytes = name.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
