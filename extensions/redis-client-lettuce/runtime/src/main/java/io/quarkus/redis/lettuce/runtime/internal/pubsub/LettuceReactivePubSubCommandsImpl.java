package io.quarkus.redis.lettuce.runtime.internal.pubsub;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.pubsub.ReactivePubSubCommands;
import io.quarkus.redis.datasource.pubsub.RedisPubSubMessage;
import io.quarkus.redis.lettuce.runtime.internal.AbstractLettuceCommands;
import io.quarkus.redis.lettuce.runtime.internal.LettuceCommand;
import io.quarkus.redis.lettuce.runtime.internal.LettuceResult;
import io.quarkus.redis.lettuce.runtime.internal.datasource.LettuceReactiveRedisDataSourceImpl;
import io.quarkus.redis.runtime.datasource.DefaultRedisPubSubMessage;
import io.quarkus.redis.runtime.datasource.Marshaller;
import io.quarkus.vertx.core.runtime.context.VertxContextSafetyToggle;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.vertx.core.Context;
import io.vertx.mutiny.core.Vertx;

/**
 * Lettuce-backed implementation of {@link ReactivePubSubCommands}.
 *
 * @param <V> the value type
 */
public class LettuceReactivePubSubCommandsImpl<V> extends AbstractLettuceCommands<V, V>
        implements ReactivePubSubCommands<V> {

    private final LettuceReactiveRedisDataSourceImpl dataSource;
    private final Supplier<CompletionStage<StatefulRedisPubSubConnection<byte[], byte[]>>> pubSubConnector;

    public LettuceReactivePubSubCommandsImpl(LettuceReactiveRedisDataSourceImpl dataSource,
            StatefulRedisConnection<byte[], byte[]> connection,
            Supplier<CompletionStage<StatefulRedisPubSubConnection<byte[], byte[]>>> pubSubConnector,
            Type classOfMessage) {
        super(connection, classOfMessage, classOfMessage, new Marshaller(classOfMessage));
        this.dataSource = dataSource;
        this.pubSubConnector = pubSubConnector;
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Void> publish(String channel, V message) {
        return _publish(channel, message).toUni();
    }

    LettuceCommand<Long, Void> _publish(String channel, V message) {
        nonNull(channel, "channel");
        nonNull(message, "message");
        return LettuceCommand.discarding(() -> async.publish(marshaller.encode(channel), marshaller.encode(message)));
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribe(String channel, Consumer<V> onMessage) {
        return subscribe(channel, onMessage, null, null);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribeToPattern(String pattern, Consumer<V> onMessage) {
        return subscribeToPattern(pattern, onMessage, null, null);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribeToPattern(String pattern, BiConsumer<String, V> onMessage) {
        return subscribeToPattern(pattern, onMessage, null, null);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribeToPatterns(List<String> patterns, Consumer<V> onMessage) {
        return subscribeToPatterns(patterns, onMessage, null, null);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribeToPatterns(List<String> patterns, BiConsumer<String, V> onMessage) {
        return subscribeToPatterns(patterns, onMessage, null, null);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribe(List<String> channels, Consumer<V> onMessage) {
        return subscribe(channels, onMessage, null, null);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribe(List<String> channels, BiConsumer<String, V> onMessage) {
        return subscribe(channels, onMessage, null, null);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribe(String channel, Consumer<V> onMessage, Runnable onEnd,
            Consumer<Throwable> onException) {
        nonNull(channel, "channel");
        return subscribe(List.of(channel), onMessage, onEnd, onException);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribeToPattern(String pattern, Consumer<V> onMessage, Runnable onEnd,
            Consumer<Throwable> onException) {
        nonNull(pattern, "pattern");
        return subscribeToPatterns(List.of(pattern), onMessage, onEnd, onException);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribeToPattern(String pattern, BiConsumer<String, V> onMessage, Runnable onEnd,
            Consumer<Throwable> onException) {
        nonNull(pattern, "pattern");
        return subscribeToPatterns(List.of(pattern), onMessage, onEnd, onException);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribeToPatterns(List<String> patterns, Consumer<V> onMessage, Runnable onEnd,
            Consumer<Throwable> onException) {
        nonNull(onMessage, "onMessage");
        return subscribeToPatterns(patterns, (ignored, value) -> onMessage.accept(value), onEnd, onException);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribeToPatterns(List<String> patterns, BiConsumer<String, V> onMessage,
            Runnable onEnd, Consumer<Throwable> onException) {
        validatePatterns(patterns);
        nonNull(onMessage, "onMessage");

        List<String> subscribed = List.copyOf(patterns);
        return LettuceResult.toUni(pubSubConnector)
                .chain(conn -> {
                    ReactiveLettucePatternSubscriber subscriber = new ReactiveLettucePatternSubscriber(conn, subscribed,
                            onMessage, onEnd,
                            onException);
                    return subscriber.subscribe().replaceWith(subscriber);
                });
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribe(List<String> channels, Consumer<V> onMessage, Runnable onEnd,
            Consumer<Throwable> onException) {
        nonNull(onMessage, "onMessage");
        return subscribe(channels, (ignored, value) -> onMessage.accept(value), onEnd, onException);
    }

    @Override
    public Uni<ReactiveRedisSubscriber> subscribe(List<String> channels, BiConsumer<String, V> onMessage, Runnable onEnd,
            Consumer<Throwable> onException) {
        validateChannels(channels);
        nonNull(onMessage, "onMessage");

        List<String> subscribed = List.copyOf(channels);
        return LettuceResult.toUni(pubSubConnector)
                .chain(conn -> {
                    ReactiveLettuceChannelSubscriber subscriber = new ReactiveLettuceChannelSubscriber(conn, subscribed,
                            onMessage, onEnd,
                            onException);
                    return subscriber.subscribe().replaceWith(subscriber);
                });
    }

    @Override
    public Multi<V> subscribe(String... channels) {
        notNullOrEmpty(channels, "channels");
        doesNotContainNull(channels, "channels");
        List<String> list = List.of(channels);
        return stream(emitter -> subscribe(list, (channel, value) -> emitter.emit(value), emitter::complete, emitter::fail));
    }

    @Override
    public Multi<RedisPubSubMessage<V>> subscribeAsMessages(String... channels) {
        notNullOrEmpty(channels, "channels");
        doesNotContainNull(channels, "channels");
        List<String> list = List.of(channels);
        return stream(emitter -> subscribe(list,
                (channel, value) -> emitter.emit(new DefaultRedisPubSubMessage<>(value, channel)),
                emitter::complete, emitter::fail));
    }

    @Override
    public Multi<V> subscribeToPatterns(String... patterns) {
        notNullOrEmpty(patterns, "patterns");
        doesNotContainNull(patterns, "patterns");
        List<String> list = List.of(patterns);
        return stream(emitter -> subscribeToPatterns(list, (channel, value) -> emitter.emit(value), emitter::complete,
                emitter::fail));
    }

    @Override
    public Multi<RedisPubSubMessage<V>> subscribeAsMessagesToPatterns(String... patterns) {
        notNullOrEmpty(patterns, "patterns");
        doesNotContainNull(patterns, "patterns");
        List<String> list = List.of(patterns);
        return stream(emitter -> subscribeToPatterns(list,
                (channel, value) -> emitter.emit(new DefaultRedisPubSubMessage<>(value, channel)),
                emitter::complete, emitter::fail));
    }

    private <T> Multi<T> stream(Function<MultiEmitter<? super T>, Uni<ReactiveRedisSubscriber>> subscription) {
        return Multi.createFrom().emitter(emitter -> subscription.apply(emitter)
                .subscribe().with(
                        subscriber -> emitter.onTermination(() -> subscriber.unsubscribe().subscribe().with(
                                ignored -> {
                                })),
                        emitter::fail));
    }

    private void validatePatterns(List<String> patterns) {
        notNullOrEmpty(patterns, "patterns");

        for (String pattern : patterns) {
            if (pattern == null) {
                throw new IllegalArgumentException("Pattern must not be null");
            }
            if (pattern.isBlank()) {
                throw new IllegalArgumentException("Pattern cannot be blank");
            }
        }
    }

    private void validateChannels(List<String> channels) {
        notNullOrEmpty(channels, "channels");

        for (String pattern : channels) {
            if (pattern == null) {
                throw new IllegalArgumentException("Channel must not be null");
            }
            if (pattern.isBlank()) {
                throw new IllegalArgumentException("Channel cannot be blank");
            }
        }
    }

    private abstract class AbstractReactiveLettuceSubscriber implements ReactiveRedisSubscriber {

        final StatefulRedisPubSubConnection<byte[], byte[]> connection;
        final List<String> names;
        final BiConsumer<String, V> onMessage;
        final Runnable onEnd;
        final Consumer<Throwable> onException;
        final Vertx vertx = dataSource.getVertx();
        private final RedisPubSubListener<byte[], byte[]> listener = listener();

        AbstractReactiveLettuceSubscriber(StatefulRedisPubSubConnection<byte[], byte[]> connection, List<String> names,
                BiConsumer<String, V> onMessage, Runnable onEnd, Consumer<Throwable> onException) {
            this.connection = connection;
            this.names = new CopyOnWriteArrayList<>(names);
            this.onMessage = onMessage;
            this.onEnd = onEnd;
            this.onException = onException;
        }

        private static byte[][] encode(List<String> names) {
            byte[][] encoded = new byte[names.size()][];
            for (int i = 0; i < encoded.length; i++) {
                encoded[i] = names.get(i).getBytes(StandardCharsets.UTF_8);
            }
            return encoded;
        }

        private void runOnDuplicatedContext(Runnable runnable) {
            Context context = VertxContext.getOrCreateDuplicatedContext(vertx.getDelegate());
            VertxContextSafetyToggle.setContextSafe(context, true);
            context.runOnContext(ignored -> runnable.run());
        }

        abstract RedisPubSubListener<byte[], byte[]> listener();

        abstract RedisFuture<Void> subscribeToRedis(byte[][] names);

        abstract RedisFuture<Void> unsubscribeFromRedis(byte[][] names);

        Uni<Void> subscribe() {
            connection.addListener(listener);
            return LettuceResult.toUni(() -> subscribeToRedis(encode(names)))
                    .onFailure().call(() -> close().onFailure().recoverWithNull())
                    .replaceWithVoid();
        }

        void deliver(byte[] channel, byte[] payload) {
            runOnDuplicatedContext(() -> {
                V value;
                try {
                    value = marshaller.decode(valueType, payload);
                } catch (RuntimeException e) {
                    if (onException == null) {
                        throw e;
                    }
                    onException.accept(e);
                    return;
                }
                onMessage.accept(new String(channel, StandardCharsets.UTF_8), value);
            });
        }

        @Override
        public Uni<Void> unsubscribe(String... names) {
            notNullOrEmpty(names, "channels");
            doesNotContainNull(names, "channels");
            List<String> list = List.of(names);
            return LettuceResult.toUni(() -> unsubscribeFromRedis(encode(list)))
                    .chain(() -> {
                        this.names.removeAll(list);
                        return closeIfNothingLeft();
                    });
        }

        @Override
        public Uni<Void> unsubscribe() {
            return LettuceResult.toUni(() -> unsubscribeFromRedis(encode(names)))
                    .chain(() -> {
                        names.clear();
                        return closeIfNothingLeft();
                    });
        }

        private Uni<Void> closeIfNothingLeft() {
            if (!names.isEmpty()) {
                return Uni.createFrom().voidItem();
            }
            return close().invoke(() -> {
                if (onEnd != null) {
                    runOnDuplicatedContext(onEnd);
                }
            });
        }

        private Uni<Void> close() {
            connection.removeListener(listener);
            return LettuceResult.toUni(connection::closeAsync).replaceWithVoid();
        }

    }

    private class ReactiveLettuceChannelSubscriber extends AbstractReactiveLettuceSubscriber {

        ReactiveLettuceChannelSubscriber(StatefulRedisPubSubConnection<byte[], byte[]> connection, List<String> channels,
                BiConsumer<String, V> onMessage, Runnable onEnd, Consumer<Throwable> onException) {
            super(connection, channels, onMessage, onEnd, onException);
        }

        @Override
        RedisPubSubListener<byte[], byte[]> listener() {
            return new RedisPubSubAdapter<>() {
                @Override
                public void message(byte[] channel, byte[] message) {
                    deliver(channel, message);
                }
            };
        }

        @Override
        RedisFuture<Void> subscribeToRedis(byte[][] channels) {
            return connection.async().subscribe(channels);
        }

        @Override
        RedisFuture<Void> unsubscribeFromRedis(byte[][] channels) {
            return connection.async().unsubscribe(channels);
        }

    }

    private class ReactiveLettucePatternSubscriber extends AbstractReactiveLettuceSubscriber {

        ReactiveLettucePatternSubscriber(StatefulRedisPubSubConnection<byte[], byte[]> connection, List<String> patterns,
                BiConsumer<String, V> onMessage, Runnable onEnd, Consumer<Throwable> onException) {
            super(connection, patterns, onMessage, onEnd, onException);
        }

        @Override
        RedisPubSubListener<byte[], byte[]> listener() {
            return new RedisPubSubAdapter<>() {
                @Override
                public void message(byte[] pattern, byte[] channel, byte[] message) {
                    deliver(channel, message);
                }
            };
        }

        @Override
        RedisFuture<Void> subscribeToRedis(byte[][] patterns) {
            return connection.async().psubscribe(patterns);
        }

        @Override
        RedisFuture<Void> unsubscribeFromRedis(byte[][] patterns) {
            return connection.async().punsubscribe(patterns);
        }

    }

}
