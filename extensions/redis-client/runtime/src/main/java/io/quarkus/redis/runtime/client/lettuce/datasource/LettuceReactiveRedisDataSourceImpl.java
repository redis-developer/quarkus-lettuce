package io.quarkus.redis.runtime.client.lettuce.datasource;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positiveOrZero;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

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
import io.quarkus.redis.runtime.datasource.OptimisticLockingTransactionResultImpl;
import io.quarkus.redis.runtime.datasource.TransactionResultImpl;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Response;

/**
 * Lettuce-backed implementation of {@link ReactiveRedisDataSource}.
 * <p>
 * Wires the {@code value} command group to {@link LettuceReactiveValueCommandsImpl} and
 * implements {@code execute(...)}, {@code flushall()} and {@code select(...)} on top of the
 * Lettuce async API. {@code withConnection(...)} runs the user block on a freshly opened
 * connection obtained from the supplied {@code connector}. {@code getRedis()} and
 * {@code withTransaction(...)} throw {@link UnsupportedOperationException} for now.
 */
public class LettuceReactiveRedisDataSourceImpl implements ReactiveRedisDataSource {

    private final Vertx vertx;
    private final StatefulRedisConnection<String, String> connection;
    private final Supplier<CompletionStage<StatefulRedisConnection<String, String>>> connector;
    private final boolean pinned;

    public LettuceReactiveRedisDataSourceImpl(Vertx vertx, StatefulRedisConnection<String, String> connection,
            Supplier<CompletionStage<StatefulRedisConnection<String, String>>> connector) {
        this(vertx, connection, connector, false);
    }

    private LettuceReactiveRedisDataSourceImpl(Vertx vertx, StatefulRedisConnection<String, String> connection,
            Supplier<CompletionStage<StatefulRedisConnection<String, String>>> connector, boolean pinned) {
        this.vertx = nonNull(vertx, "vertx");
        this.connection = nonNull(connection, "connection");
        this.connector = connector;
        this.pinned = pinned;
    }

    static LettuceReactiveRedisDataSourceImpl pinnedTo(Vertx vertx, StatefulRedisConnection<String, String> connection) {
        return new LettuceReactiveRedisDataSourceImpl(vertx, connection, null, true);
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
                .map(ignored -> output.toVertxResponse());
    }

    static ProtocolKeyword resolve(String name) {
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

    /**
     * Opens a fresh connection from the connector, without blocking the caller.
     */
    Uni<StatefulRedisConnection<String, String>> openConnection() {
        return LettuceResult.toUni(connector::get);
    }

    @Override
    public Uni<Void> withConnection(Function<ReactiveRedisDataSource, Uni<Void>> function) {
        if (pinned) {
            return function.apply(this);
        }
        return openConnection()
                .onItem().transformToUni(conn -> {
                    LettuceReactiveRedisDataSourceImpl pinnedDs = pinnedTo(vertx, conn);
                    return Uni.createFrom().deferred(() -> function.apply(pinnedDs))
                            .onTermination().call(() -> LettuceResult.toUni(conn::closeAsync).replaceWithVoid());
                });
    }

    @Override
    public Uni<TransactionResult> withTransaction(Function<ReactiveTransactionalRedisDataSource, Uni<Void>> tx) {
        nonNull(tx, "tx");
        return withTxConnection(conn -> runTx(conn, tx, null));
    }

    @Override
    public Uni<TransactionResult> withTransaction(Function<ReactiveTransactionalRedisDataSource, Uni<Void>> tx,
            String... watchedKeys) {
        nonNull(tx, "tx");
        notNullOrEmpty(watchedKeys, "watchedKeys");
        doesNotContainNull(watchedKeys, "watchedKeys");
        return withTxConnection(conn -> runTx(conn, tx, watchedKeys));
    }

    @Override
    public <I> Uni<OptimisticLockingTransactionResult<I>> withTransaction(
            Function<ReactiveRedisDataSource, Uni<I>> preTx,
            BiFunction<I, ReactiveTransactionalRedisDataSource, Uni<Void>> tx,
            String... watchedKeys) {
        nonNull(preTx, "preTx");
        nonNull(tx, "tx");
        notNullOrEmpty(watchedKeys, "watchedKeys");
        doesNotContainNull(watchedKeys, "watchedKeys");
        return withTxConnection(conn -> runOptimisticTx(conn, preTx, tx, watchedKeys));
    }

    /**
     * Runs {@code body} on a transaction-scoped connection. When this data source is already
     * pinned (nested inside {@code withConnection}), the pinned connection is reused and left
     * open for the outer scope to release. Otherwise a fresh connection is opened via the
     * {@code connector} and closed on every termination path.
     */
    private <T> Uni<T> withTxConnection(Function<StatefulRedisConnection<String, String>, Uni<T>> body) {
        if (pinned) {
            return Uni.createFrom().deferred(() -> body.apply(connection));
        }
        return openConnection()
                .onItem().transformToUni(conn -> Uni.createFrom().deferred(() -> body.apply(conn))
                        .onTermination().call(() -> LettuceResult.toUni(conn::closeAsync).replaceWithVoid()));
    }

    private Uni<TransactionResult> runTx(StatefulRedisConnection<String, String> conn,
            Function<ReactiveTransactionalRedisDataSource, Uni<Void>> tx, String[] watchedKeys) {
        LettuceReactiveRedisDataSourceImpl pinnedDs = pinnedTo(vertx, conn);
        LettuceTransactionHolder holder = new LettuceTransactionHolder();
        LettuceReactiveTransactionalRedisDataSourceImpl txDs = new LettuceReactiveTransactionalRedisDataSourceImpl(
                pinnedDs, holder);

        Uni<Void> watch = watchedKeys == null ? Uni.createFrom().voidItem() : watch(conn, watchedKeys);
        return watch
                .chain(() -> LettuceResult.toUni(() -> conn.async().multi()).replaceWithVoid())
                .chain(() -> Uni.createFrom().deferred(() -> tx.apply(txDs)))
                .onItemOrFailure().transformToUni((x, failure) -> {
                    if (failure != null) {
                        return abort(conn, holder, failure);
                    }
                    if (holder.discarded()) {
                        return Uni.createFrom().item(TransactionResultImpl.DISCARDED);
                    }
                    return LettuceResult.toUni(() -> conn.async().exec())
                            .chain(execResult -> execResult == null || execResult.wasDiscarded()
                                    ? Uni.createFrom().item(TransactionResultImpl.DISCARDED)
                                    : holder.toResult());
                });
    }

    private <I> Uni<OptimisticLockingTransactionResult<I>> runOptimisticTx(StatefulRedisConnection<String, String> conn,
            Function<ReactiveRedisDataSource, Uni<I>> preTx,
            BiFunction<I, ReactiveTransactionalRedisDataSource, Uni<Void>> tx, String[] watchedKeys) {
        LettuceReactiveRedisDataSourceImpl pinnedDs = pinnedTo(vertx, conn);
        LettuceTransactionHolder holder = new LettuceTransactionHolder();
        LettuceReactiveTransactionalRedisDataSourceImpl txDs = new LettuceReactiveTransactionalRedisDataSourceImpl(
                pinnedDs, holder);

        return watch(conn, watchedKeys)
                .chain(() -> Uni.createFrom().deferred(() -> preTx.apply(pinnedDs)))
                .onFailure().recoverWithUni(failure -> LettuceResult.toUni(() -> conn.async().unwatch())
                        .onItemOrFailure().transformToUni((r, f) -> {
                            if (f != null) {
                                failure.addSuppressed(f);
                            }
                            return Uni.createFrom().failure(failure);
                        }))
                .chain(input -> LettuceResult.toUni(() -> conn.async().multi()).replaceWithVoid()
                        .chain(() -> Uni.createFrom().deferred(() -> tx.apply(input, txDs)))
                        .onItemOrFailure().transformToUni((x, failure) -> {
                            if (failure != null) {
                                return abort(conn, holder, failure);
                            }
                            if (holder.discarded()) {
                                return Uni.createFrom().item(OptimisticLockingTransactionResultImpl.discarded(input));
                            }
                            return LettuceResult.toUni(() -> conn.async().exec())
                                    .chain(execResult -> execResult == null || execResult.wasDiscarded()
                                            ? Uni.createFrom()
                                                    .item(OptimisticLockingTransactionResultImpl.discarded(input))
                                            : holder.toOptimisticLockingResult(input));
                        }));
    }

    private Uni<Void> watch(StatefulRedisConnection<String, String> conn, String[] keys) {
        return LettuceResult.toUni(() -> conn.async().watch(keys)).replaceWithVoid();
    }

    /**
     * Aborts an in-flight transaction after the user block failed: issues {@code DISCARD}
     * (unless the user already discarded) and re-propagates the original failure, attaching any
     * {@code DISCARD} failure as suppressed. Mirrors the Vert.x backend's abort path.
     */
    private static <T> Uni<T> abort(StatefulRedisConnection<String, String> conn, LettuceTransactionHolder holder,
            Throwable failure) {
        if (holder.discarded()) {
            return Uni.createFrom().failure(failure);
        }
        return LettuceResult.toUni(() -> conn.async().discard())
                .onItemOrFailure().transformToUni((r, f) -> {
                    if (f != null) {
                        failure.addSuppressed(f);
                    }
                    return Uni.createFrom().failure(failure);
                });
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
