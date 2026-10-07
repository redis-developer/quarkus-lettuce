package io.quarkus.redis.lettuce.runtime.internal.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.pubsub.ReactivePubSubCommands;
import io.quarkus.redis.datasource.pubsub.ReactivePubSubCommands.ReactiveRedisSubscriber;
import io.quarkus.redis.datasource.pubsub.RedisPubSubMessage;
import io.quarkus.redis.lettuce.runtime.internal.CommandsTestBase;
import io.quarkus.redis.lettuce.runtime.internal.Person;
import io.quarkus.redis.runtime.datasource.DefaultRedisPubSubMessage;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.subscription.Cancellable;

class LettucePubSubCommandsTest extends CommandsTestBase {

    final String channel = "channel-" + UUID.randomUUID();
    final Person luke = new Person("luke", "skywalker");
    final Person leia = new Person("leia", "skywalker");

    ReactiveRedisDataSource reactiveDs;
    ReactivePubSubCommands<Person> pubsub;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        pubsub = reactiveDs.pubsub(Person.class);
    }

    @AfterEach
    void noChannelLeftSubscribed() {
        Awaitility.await().untilAsserted(() -> assertThat(connection.sync().pubsubChannels()).isEmpty());
    }

    @Test
    void getDataSource() {
        assertThat(pubsub.getDataSource()).isSameAs(reactiveDs);
    }

    @Test
    void publishWithoutSubscriberCompletes() {
        pubsub.publish(channel, luke).await().atMost(TIMEOUT);
    }

    @Test
    void subscribeReceivesPublishedMessagesAndUnsubscribeClosesItsConnection() {
        long before = connectionCount();
        List<Person> people = new CopyOnWriteArrayList<>();
        List<Boolean> onDuplicatedContext = new CopyOnWriteArrayList<>();

        ReactiveRedisSubscriber subscriber = pubsub.subscribe(channel, person -> {
            onDuplicatedContext.add(VertxContext.isOnDuplicatedContext());
            people.add(person);
        }).await().atMost(TIMEOUT);
        // Each subscriber owns a dedicated Pub/Sub connection, apart from the shared one and the pool.
        assertThat(connectionCount()).isEqualTo(before + 1);
        assertThat(connection.sync().pubsubChannels()).hasSize(1);

        pubsub.publish(channel, luke).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> people.size() == 1);
        pubsub.publish(channel, leia).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> people.size() == 2);

        assertThat(people).containsExactly(luke, leia);
        assertThat(onDuplicatedContext).containsOnly(true);

        subscriber.unsubscribe().await().atMost(TIMEOUT);
        Awaitility.await().until(() -> connectionCount() == before);
    }

    @Test
    void messagesOnOtherChannelsAreNotDelivered() {
        List<Person> people = new CopyOnWriteArrayList<>();
        ReactiveRedisSubscriber subscriber = pubsub.subscribe(channel, people::add).await().atMost(TIMEOUT);

        pubsub.publish(channel + "-other", luke).await().atMost(TIMEOUT);
        pubsub.publish(channel, leia).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> people.size() == 1);
        assertThat(people).containsExactly(leia);

        subscriber.unsubscribe().await().atMost(TIMEOUT);
    }

    @Test
    void connectionIsClosedAndOnEndCalledOnlyAfterTheLastChannelIsUnsubscribed() {
        long before = connectionCount();
        String second = channel + "-second";
        List<String> receivedOn = new CopyOnWriteArrayList<>();
        AtomicInteger ended = new AtomicInteger();

        ReactiveRedisSubscriber subscriber = pubsub
                .subscribe(List.of(channel, second), (name, person) -> receivedOn.add(name), ended::incrementAndGet, null)
                .await().atMost(TIMEOUT);
        assertThat(connectionCount()).isEqualTo(before + 1);

        subscriber.unsubscribe(channel).await().atMost(TIMEOUT);
        assertThat(connectionCount()).isEqualTo(before + 1);
        assertThat(ended).hasValue(0);

        pubsub.publish(channel, luke).await().atMost(TIMEOUT);
        pubsub.publish(second, leia).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> receivedOn.size() == 1);
        assertThat(receivedOn).containsExactly(second);

        subscriber.unsubscribe(second).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> connectionCount() == before);
        Awaitility.await().until(() -> ended.get() == 1);
    }

    @Test
    void invalidArgumentsAreRejectedEagerly() {
        assertThatThrownBy(() -> pubsub.publish(null, luke)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pubsub.publish(channel, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pubsub.subscribe((String) null, p -> {
        })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pubsub.subscribe(channel, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pubsub.subscribe(List.of(" "), (c, p) -> {
        }, null, null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("blank");
        assertThatThrownBy(() -> pubsub.subscribe(List.of(), (c, p) -> {
        }, null, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void channelAwareCallbackReceivesTheChannelName() {
        List<RedisPubSubMessage<Person>> messages = new CopyOnWriteArrayList<>();
        ReactiveRedisSubscriber subscriber = pubsub
                .subscribe(List.of(channel), (name, person) -> messages.add(new DefaultRedisPubSubMessage<>(person, name)))
                .await().atMost(TIMEOUT);

        pubsub.publish(channel, luke).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> messages.size() == 1);
        assertThat(messages.get(0).getChannel()).isEqualTo(channel);
        assertThat(messages.get(0).getPayload()).isEqualTo(luke);

        subscriber.unsubscribe().await().atMost(TIMEOUT);
    }

    @Test
    void patternSubscriptionReceivesMatchingChannelsAndReportsTheChannelName() {
        long before = connectionCount();
        List<String> channels = new CopyOnWriteArrayList<>();
        List<Person> people = new CopyOnWriteArrayList<>();
        List<Boolean> onDuplicatedContext = new CopyOnWriteArrayList<>();

        ReactiveRedisSubscriber subscriber = pubsub.subscribeToPattern(channel + "-*", (name, person) -> {
            onDuplicatedContext.add(VertxContext.isOnDuplicatedContext());
            channels.add(name);
            people.add(person);
        }).await().atMost(TIMEOUT);
        assertThat(connectionCount()).isEqualTo(before + 1);
        assertThat(connection.sync().pubsubNumpat()).isEqualTo(1);

        pubsub.publish(channel, luke).await().atMost(TIMEOUT); // does not match the pattern
        pubsub.publish(channel + "-a", luke).await().atMost(TIMEOUT);
        pubsub.publish(channel + "-b", leia).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> people.size() == 2);

        assertThat(people).containsExactly(luke, leia);
        assertThat(channels).containsExactly(channel + "-a", channel + "-b");
        assertThat(onDuplicatedContext).containsOnly(true);

        subscriber.unsubscribe().await().atMost(TIMEOUT);
        Awaitility.await().until(() -> connectionCount() == before);
        assertThat(connection.sync().pubsubNumpat()).isZero();
    }

    @Test
    void patternOnEndIsCalledAfterTheLastPatternIsUnsubscribed() {
        String first = channel + "-a*";
        String second = channel + "-b*";
        List<Person> people = new CopyOnWriteArrayList<>();
        AtomicInteger ended = new AtomicInteger();

        ReactiveRedisSubscriber subscriber = pubsub
                .subscribeToPatterns(List.of(first, second), (Consumer<Person>) people::add, ended::incrementAndGet, null)
                .await().atMost(TIMEOUT);

        subscriber.unsubscribe(first).await().atMost(TIMEOUT);
        assertThat(ended).hasValue(0);
        pubsub.publish(channel + "-a1", luke).await().atMost(TIMEOUT);
        pubsub.publish(channel + "-b1", leia).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> people.size() == 1);
        assertThat(people).containsExactly(leia);

        subscriber.unsubscribe(second).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> ended.get() == 1);
    }

    @Test
    void multiSubscribeEmitsMessagesAndCancellationClosesTheConnection() {
        long before = connectionCount();
        List<Person> people = new CopyOnWriteArrayList<>();

        Cancellable cancellable = pubsub.subscribe(channel).subscribe().with(people::add);
        Awaitility.await().until(() -> connectionCount() == before + 1);

        pubsub.publish(channel, luke).await().atMost(TIMEOUT);
        pubsub.publish(channel, leia).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> people.size() == 2);
        assertThat(people).containsExactly(luke, leia);

        cancellable.cancel();
        Awaitility.await().until(() -> connectionCount() == before);
    }

    @Test
    void multiSubscribeAsMessagesCarriesTheChannel() {
        String second = channel + "-second";
        List<RedisPubSubMessage<Person>> messages = new CopyOnWriteArrayList<>();

        Cancellable cancellable = pubsub.subscribeAsMessages(channel, second).subscribe().with(messages::add);
        Awaitility.await().until(() -> connection.sync().pubsubChannels().size() == 2);

        pubsub.publish(channel, luke).await().atMost(TIMEOUT);
        pubsub.publish(second, leia).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> messages.size() == 2);
        assertThat(messages).extracting(RedisPubSubMessage::getChannel).containsExactly(channel, second);
        assertThat(messages).extracting(RedisPubSubMessage::getPayload).containsExactly(luke, leia);

        cancellable.cancel();
    }

    @Test
    void multiSubscribeToPatternsEmitsMatchingMessages() {
        long before = connectionCount();
        List<Person> people = new CopyOnWriteArrayList<>();
        List<RedisPubSubMessage<Person>> messages = new CopyOnWriteArrayList<>();

        Cancellable values = pubsub.subscribeToPatterns(channel + "-*").subscribe().with(people::add);
        Cancellable withChannel = pubsub.subscribeAsMessagesToPatterns(channel + "-*").subscribe().with(messages::add);
        Awaitility.await().until(() -> connectionCount() == before + 2);

        pubsub.publish(channel + "-x", luke).await().atMost(TIMEOUT);
        Awaitility.await().until(() -> people.size() == 1 && messages.size() == 1);
        assertThat(people).containsExactly(luke);
        assertThat(messages.get(0).getChannel()).isEqualTo(channel + "-x");
        assertThat(messages.get(0).getPayload()).isEqualTo(luke);

        values.cancel();
        withChannel.cancel();
        Awaitility.await().until(() -> connectionCount() == before);
    }

    @Test
    void multiCancelledBeforeTheSubscriptionIsEstablishedStillReleasesTheConnection() {
        long before = connectionCount();
        pubsub.subscribe(channel).subscribe().with(ignored -> {
        }).cancel();
        // The connection may be opened after the cancellation; it must still be closed afterwards.
        Awaitility.await().during(Duration.ofMillis(300)).until(() -> connectionCount() == before);
    }

    @Test
    void invalidPatternArgumentsAreRejectedEagerly() {
        assertThatThrownBy(() -> pubsub.subscribeToPattern((String) null, p -> {
        })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pubsub.subscribeToPattern(channel, (Consumer<Person>) null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pubsub.subscribeToPatterns(List.of(" "), (c, p) -> {
        }, null, null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("blank");
        assertThatThrownBy(() -> pubsub.subscribeToPatterns(List.of(), p -> {
        })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pubsub.subscribe(new String[0])).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pubsub.subscribeToPatterns(channel, null)).isInstanceOf(IllegalArgumentException.class);
    }

}
