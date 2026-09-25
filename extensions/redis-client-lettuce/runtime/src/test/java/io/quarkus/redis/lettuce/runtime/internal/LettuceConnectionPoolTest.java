package io.quarkus.redis.lettuce.runtime.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.lettuce.core.api.StatefulRedisConnection;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;

class LettuceConnectionPoolTest extends CommandsTestBase {

    @Test
    void acquireReleaseReusesConnection() {
        LettuceConnectionPool pool = pool(1, 1);
        StatefulRedisConnection<byte[], byte[]> first = pool.acquireBlocking(TIMEOUT);
        long firstId = first.sync().clientId();
        pool.release(first).await().atMost(TIMEOUT);

        StatefulRedisConnection<byte[], byte[]> second = pool.acquireBlocking(TIMEOUT);
        assertThat(second.sync().clientId()).isEqualTo(firstId);
        pool.release(second).await().atMost(TIMEOUT);
    }

    @Test
    void cancellationDequeuesWaiterFreeingItsWaitingSlot() {
        LettuceConnectionPool pool = pool(1, 1);
        StatefulRedisConnection<byte[], byte[]> held = pool.acquireBlocking(TIMEOUT);

        Cancellable firstWaiter = pool.withPooled(conn -> Uni.createFrom().voidItem()).subscribe().with(c -> {
        }, t -> {
        });
        firstWaiter.cancel();

        // The canceled waiter must have freed its slot: this one queues instead of being rejected,
        // proving the waiting counter was decremented on cancellation rather than leaked.
        AtomicBoolean secondWaiterCompleted = new AtomicBoolean();
        AtomicReference<Throwable> secondWaiterFailure = new AtomicReference<>();
        pool.withPooled(conn -> Uni.createFrom().voidItem())
                .subscribe().with(c -> secondWaiterCompleted.set(true), secondWaiterFailure::set);
        assertThat(secondWaiterFailure.get()).isNull();
        assertThat(secondWaiterCompleted).isFalse();

        pool.release(held).await().atMost(TIMEOUT);
        await().atMost(TIMEOUT).until(secondWaiterCompleted::get);
        assertThat(secondWaiterFailure.get()).isNull();
        await().atMost(TIMEOUT).until(() -> pool.getIdle() == 1);
    }

    @Test
    void closeCausesFurtherAcquireToFail() {
        LettuceConnectionPool pool = pool(1, 1);
        pool.close().await().atMost(TIMEOUT);
        assertThatThrownBy(() -> pool.acquireBlocking(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void closeFailsQueuedWaitersInsteadOfLeavingThemHanging() throws Exception {
        LettuceConnectionPool pool = pool(1, 1);
        pool.acquireBlocking(TIMEOUT); // hold the only connection so the next caller queues

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                pool.acquireBlocking(TIMEOUT);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        waiter.start();
        await().atMost(TIMEOUT).until(() -> waiter.getState() == Thread.State.TIMED_WAITING);

        pool.close().await().atMost(TIMEOUT);

        // Well within the waiter's own timeout: it was failed by close(), not by giving up.
        waiter.join(Duration.ofSeconds(1).toMillis());
        assertThat(waiter.isAlive()).isFalse();
        assertThat(failure.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
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
    void cancellingWithPooledWhileThePoolIsStillConnectingReturnsTheConnection() {
        // Gate the connector so the cancel provably lands while the underlying connect is in flight.
        CompletableFuture<Void> gate = new CompletableFuture<>();
        LettuceConnectionPool pool = new LettuceConnectionPool(() -> gate.thenCompose(v -> connectAsync()), 1, 1);

        AtomicBoolean bodyRan = new AtomicBoolean();
        Cancellable subscription = pool.withPooled(conn -> {
            bodyRan.set(true);
            return Uni.createFrom().voidItem();
        }).subscribe().with(x -> {
        }, t -> {
        });
        subscription.cancel();
        gate.complete(null);

        // The connection the pool finished creating for the cancelled caller must end up idle in
        // the pool, not handed to a dead request (a silent no-op) and thereby leaked.
        await().atMost(TIMEOUT).until(() -> pool.getObjectCount() == 1);
        await().atMost(TIMEOUT).until(() -> pool.getIdle() == 1);
        assertThat(bodyRan).isFalse();

        // With maxTotal=1 this would queue forever had the connection leaked.
        StatefulRedisConnection<byte[], byte[]> conn = pool.acquireBlocking(TIMEOUT);
        pool.release(conn).await().atMost(TIMEOUT);
    }

    @Test
    void waiterCancellingConcurrentlyWithReleaseNeverLeaksTheConnection() throws Exception {
        // release() dequeues a waiter and then completes it. A cancel landing in that window used
        // to make the hand-over a silent no-op, leaking the connection. There is no hook to inject
        // into that window, so race the two from separate threads many times: the connection must
        // always end up idle again, whichever side wins. (Handing over through a Uni item cannot
        // pass this test: Mutiny drops an item that arrives after a cancel without telling the
        // emitter — hence every request hands over through a callback that reports whether it
        // was still live.)
        LettuceConnectionPool pool = pool(1, 1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 300; i++) {
                StatefulRedisConnection<byte[], byte[]> held = pool.acquireBlocking(TIMEOUT);
                Cancellable waiter = pool.withPooled(conn -> Uni.createFrom().voidItem()).subscribe().with(x -> {
                }, t -> {
                });

                CyclicBarrier barrier = new CyclicBarrier(2);
                Future<?> cancel = executor.submit(() -> {
                    barrier.await();
                    waiter.cancel();
                    return null;
                });
                Future<?> release = executor.submit(() -> {
                    barrier.await();
                    pool.release(held).await().atMost(TIMEOUT);
                    return null;
                });
                cancel.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                release.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(1)).until(() -> pool.getIdle() == 1);
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(pool.getObjectCount()).isEqualTo(1);
    }

    @Test
    void releaseRacingAnExhaustedAcquireNeverLosesTheWakeUp() throws Exception {
        // Lost wake-up: an acquire finds the pool exhausted, and before it is queued a concurrent
        // release() finds no waiter and returns the connection to Lettuce's idle cache. The queued
        // caller would then wait for a release that never comes. The pool lock makes the two
        // decisions mutually exclusive; there is no hook into the window, so race them many times.
        LettuceConnectionPool pool = pool(1, 1);
        // Mutiny's context-propagation hook is registered lazily on first use and is not safe to
        // race from two threads; run through it once here so the race below only races the pool.
        pool.release(pool.acquireBlocking(TIMEOUT)).await().atMost(TIMEOUT);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 300; i++) {
                StatefulRedisConnection<byte[], byte[]> held = pool.acquireBlocking(TIMEOUT);

                CyclicBarrier barrier = new CyclicBarrier(2);
                Future<?> release = executor.submit(() -> {
                    barrier.await();
                    pool.release(held).await().atMost(TIMEOUT);
                    return null;
                });
                Future<Long> acquire = executor.submit(() -> {
                    barrier.await();
                    return pool.withPooled(conn -> LettuceResult.toUni(() -> conn.async().clientId()))
                            .await().atMost(TIMEOUT);
                });
                release.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(acquire.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isNotNull();

                await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(1)).until(() -> pool.getIdle() == 1);
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(pool.getObjectCount()).isEqualTo(1);
    }

    @Test
    void acquireBlockingTimingOutWhileThePoolIsStillConnectingReturnsTheConnection() {
        CompletableFuture<Void> gate = new CompletableFuture<>();
        LettuceConnectionPool pool = new LettuceConnectionPool(() -> gate.thenCompose(v -> connectAsync()), 1, 1);

        assertThatThrownBy(() -> pool.acquireBlocking(Duration.ofMillis(200)))
                .isInstanceOf(io.smallrye.mutiny.TimeoutException.class);
        gate.complete(null);

        // The connection that finished connecting after the caller gave up goes back to the pool.
        await().atMost(TIMEOUT).until(() -> pool.getIdle() == 1);

        StatefulRedisConnection<byte[], byte[]> conn = pool.acquireBlocking(TIMEOUT);
        assertThat(conn.sync().ping()).isEqualTo("PONG");
        pool.release(conn).await().atMost(TIMEOUT);
    }

    @Test
    void acquireBlockingQueuesWhenExhaustedAndFailsWhenTooManyWait() {
        LettuceConnectionPool pool = pool(1, 1);
        StatefulRedisConnection<byte[], byte[]> held = pool.acquireBlocking(TIMEOUT);

        // First extra caller queues (maxWaiting=1) and times out; the slot it held must be freed...
        assertThatThrownBy(() -> pool.acquireBlocking(Duration.ofMillis(200)))
                .isInstanceOf(io.smallrye.mutiny.TimeoutException.class);
        // ...so this one queues too, instead of being rejected as "too many waiting".
        AtomicReference<StatefulRedisConnection<byte[], byte[]>> handedOver = new AtomicReference<>();
        Thread waiter = new Thread(() -> handedOver.set(pool.acquireBlocking(TIMEOUT)));
        waiter.start();
        await().atMost(TIMEOUT).until(() -> waiter.getState() == Thread.State.TIMED_WAITING);

        // With the queue full (maxWaiting=1), a further caller is rejected immediately.
        assertThatThrownBy(() -> pool.acquireBlocking(TIMEOUT))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("too many waiting requests");

        pool.release(held).await().atMost(TIMEOUT);
        await().atMost(TIMEOUT).until(() -> handedOver.get() != null);
        assertThat(handedOver.get()).isSameAs(held);
        pool.release(handedOver.get()).await().atMost(TIMEOUT);
    }

    @Test
    void cancellingWithScopedCancelsTheBodyAndReleasesTheConnection() {
        LettuceConnectionPool pool = pool(1, 1);
        AtomicBoolean bodyCancelled = new AtomicBoolean();

        Cancellable subscription = pool
                .withScoped(conn -> Uni.createFrom().<Void> emitter(e -> {
                    // never completes
                }).onCancellation().invoke(() -> bodyCancelled.set(true)))
                .subscribe().with(x -> {
                }, t -> {
                });
        await().atMost(TIMEOUT).until(() -> pool.getObjectCount() == 1 && pool.getIdle() == 0);

        subscription.cancel();

        // Unlike withPooled, a scoped connection follows the caller: cancelling propagates to the
        // body and frees the connection right away.
        assertThat(bodyCancelled).isTrue();
        await().atMost(TIMEOUT).until(() -> pool.getIdle() == 1);
    }

    @Test
    void cancellingWithScopedBeforeHandOverReturnsTheConnection() {
        CompletableFuture<Void> gate = new CompletableFuture<>();
        LettuceConnectionPool pool = new LettuceConnectionPool(() -> gate.thenCompose(v -> connectAsync()), 1, 1);
        AtomicBoolean bodyRan = new AtomicBoolean();

        Cancellable subscription = pool.withScoped(conn -> {
            bodyRan.set(true);
            return Uni.createFrom().voidItem();
        }).subscribe().with(x -> {
        }, t -> {
        });
        subscription.cancel();
        gate.complete(null);

        await().atMost(TIMEOUT).until(() -> pool.getIdle() == 1);
        assertThat(bodyRan).isFalse();
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
