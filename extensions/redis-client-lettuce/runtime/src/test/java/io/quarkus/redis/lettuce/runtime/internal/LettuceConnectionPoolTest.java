package io.quarkus.redis.lettuce.runtime.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.NoSuchElementException;
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

        // The cancelled waiter must have freed its slot: this one queues instead of being rejected,
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

}
