package io.quarkus.redis.lettuce.runtime.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.lettuce.core.api.StatefulRedisConnection;
import io.smallrye.mutiny.subscription.Cancellable;

class LettuceConnectionPoolTest extends CommandsTestBase {

    @Test
    void acquireReleaseReusesConnection() {
        LettuceConnectionPool pool = pool(1, 1);
        StatefulRedisConnection<byte[], byte[]> first = pool.acquire().await().atMost(TIMEOUT);
        long firstId = first.sync().clientId();
        pool.release(first).await().atMost(TIMEOUT);

        StatefulRedisConnection<byte[], byte[]> second = pool.acquire().await().atMost(TIMEOUT);
        assertThat(second.sync().clientId()).isEqualTo(firstId);
        pool.release(second).await().atMost(TIMEOUT);
    }

    @Test
    void exhaustionQueuesWaiterAndReleaseHandsItOver() {
        LettuceConnectionPool pool = pool(1, 1);
        StatefulRedisConnection<byte[], byte[]> held = pool.acquire().await().atMost(TIMEOUT);
        long heldId = held.sync().clientId();

        AtomicReference<StatefulRedisConnection<byte[], byte[]>> waiterResult = new AtomicReference<>();
        Cancellable subscription = pool.acquire().subscribe().with(waiterResult::set, t -> {
        });

        // The pool is exhausted (maxTotal=1): the second acquire queues instead of failing.
        assertThat(waiterResult.get()).isNull();
        pool.release(held).await().atMost(TIMEOUT);

        await().atMost(TIMEOUT).until(() -> waiterResult.get() != null);
        assertThat(waiterResult.get().sync().clientId()).isEqualTo(heldId);
        subscription.cancel();
        pool.release(waiterResult.get()).await().atMost(TIMEOUT);
    }

    @Test
    void queueBeyondMaxWaitingFailsWithClearMessage() {
        LettuceConnectionPool pool = pool(1, 1);
        StatefulRedisConnection<byte[], byte[]> held = pool.acquire().await().atMost(TIMEOUT);
        // First extra caller queues (maxWaiting=1); a second one must be rejected immediately.
        pool.acquire().subscribe().with(c -> {
        }, t -> {
        });
        assertThatThrownBy(() -> pool.acquire().await().atMost(TIMEOUT))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("too many waiting requests");
        pool.release(held).await().atMost(TIMEOUT);
    }

    @Test
    void cancellationDequeuesWaiterFreeingItsWaitingSlot() {
        LettuceConnectionPool pool = pool(1, 1);
        StatefulRedisConnection<byte[], byte[]> held = pool.acquire().await().atMost(TIMEOUT);

        Cancellable firstWaiter = pool.acquire().subscribe().with(c -> {
        }, t -> {
        });
        firstWaiter.cancel();

        // The canceled waiter must have freed its slot: this one queues instead of being rejected,
        // proving the waiting counter was decremented on cancellation rather than leaked.
        AtomicReference<StatefulRedisConnection<byte[], byte[]>> secondWaiterResult = new AtomicReference<>();
        AtomicReference<Throwable> secondWaiterFailure = new AtomicReference<>();
        pool.acquire().subscribe().with(secondWaiterResult::set, secondWaiterFailure::set);
        assertThat(secondWaiterFailure.get()).isNull();

        pool.release(held).await().atMost(TIMEOUT);
        await().atMost(TIMEOUT).until(() -> secondWaiterResult.get() != null);
        pool.release(secondWaiterResult.get()).await().atMost(TIMEOUT);
    }

    @Test
    void closeCausesFurtherAcquireToFail() {
        LettuceConnectionPool pool = pool(1, 1);
        pool.close().await().atMost(TIMEOUT);
        assertThatThrownBy(() -> pool.acquire().await().atMost(TIMEOUT))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void cancellingWithPooledDoesNotFreeConnectionUntilTheRealCommandCompletes() {
        LettuceConnectionPool pool = pool(1, 1);
        String missingKey = UUID.randomUUID().toString();

        Cancellable subscription = pool
                .withPooled(conn -> LettuceResult
                        .toUni(() -> conn.async().blpop(30L, missingKey.getBytes(StandardCharsets.UTF_8))))
                .subscribe().with(item -> {
                }, failure -> {
                });
        // Wait for the connection to actually be borrowed (and the BLPOP dispatched) before
        // cancelling, so this exercises "cancel while a real command is in flight" rather than
        // racing the still-in-progress connect.
        await().atMost(TIMEOUT).until(() -> pool.getObjectCount() == 1);
        subscription.cancel();

        // The caller gave up, but the BLPOP is still pending on the wire: the connection must stay
        // checked out, not be returned to the pool as idle.
        assertThat(pool.getIdle()).isZero();

        // Only the real Redis reply frees the connection.
        rawPush(missingKey, "unblocked");
        await().atMost(TIMEOUT).until(() -> pool.getIdle() == 1);
    }

    @Test
    void withPooledReleasesConnectionWhenBodyThrowsSynchronously() {
        LettuceConnectionPool pool = pool(1, 1);
        IllegalArgumentException boom = new IllegalArgumentException("bad args");

        // The body throws from apply() itself (like eager argument validation), before ever
        // returning a Uni to subscribe to. The failure must reach the caller...
        assertThatThrownBy(() -> pool.<Void> withPooled(conn -> {
            throw boom;
        }).await().atMost(TIMEOUT)).isSameAs(boom);

        // ...and the connection must be back in the pool, not leaked as checked-out forever.
        assertThat(pool.getObjectCount()).isEqualTo(1);
        assertThat(pool.getIdle()).isEqualTo(1);

        // With maxTotal=1, this would queue forever (and time out) had the connection leaked.
        Long pong = pool.withPooled(conn -> LettuceResult.toUni(() -> conn.async().clientId()))
                .await().atMost(TIMEOUT);
        assertThat(pong).isNotNull();
        assertThat(pool.getIdle()).isEqualTo(1);
    }

    private static void rawPush(String key, String value) {
        connection.sync().rpush(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

}
