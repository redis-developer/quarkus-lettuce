package io.quarkus.redis.lettuce.runtime.internal;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;

/**
 * Bounded pool of {@code StatefulRedisConnection<byte[], byte[]>}, used for blocking commands
 * and scoped connections ({@code withConnection}/{@code withTransaction}) so that they never
 * occupy the shared multiplexed connection used by ordinary commands.
 * <p>
 * {@link BoundedAsyncPool#acquire()} fails immediately with a {@link NoSuchElementException}
 * once {@code maxTotal} connections are checked out — it has no waiting queue. This class adds
 * one, bounded by {@code maxWaiting}, mirroring the Vert.x backend's {@code max-pool-waiting}.
 */
public final class LettuceConnectionPool {

    private static final Logger LOGGER = Logger.getLogger(LettuceConnectionPool.class);

    private final BoundedAsyncPool<StatefulRedisConnection<byte[], byte[]>> pool;
    private final int maxWaiting;
    private final Queue<Request> waiters = new ConcurrentLinkedQueue<>();
    private final AtomicInteger waiting = new AtomicInteger();

    public LettuceConnectionPool(Supplier<CompletionStage<StatefulRedisConnection<byte[], byte[]>>> connector,
            int maxPoolSize, int maxWaiting) {
        BoundedPoolConfig poolConfig = BoundedPoolConfig.builder()
                .maxTotal(maxPoolSize)
                .maxIdle(maxPoolSize)
                .build();
        this.pool = AsyncConnectionPoolSupport.createBoundedObjectPool(connector, poolConfig, false);
        this.maxWaiting = maxWaiting;
    }

    /**
     * Blocks until a connection is available or {@code timeout} elapses. On timeout (or interrupt)
     * the request is withdrawn from the queue and a connection that arrives just as the caller
     * gives up is returned to the pool rather than leaked.
     *
     * @throws io.smallrye.mutiny.TimeoutException if no connection could be obtained in time
     */
    public StatefulRedisConnection<byte[], byte[]> acquireBlocking(Duration timeout) {
        Request request = new Request();
        request.start();
        try {
            return request.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            request.abandon();
            request.thenAccept(this::returnToPool);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
            throw new io.smallrye.mutiny.TimeoutException();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new CompletionException(e.getCause());
        }
    }

    /**
     * Releases a connection back to the pool, handing it straight to the next waiter if any.
     * Waiters that were abandoned between being dequeued and being completed are skipped.
     */
    public Uni<Void> release(StatefulRedisConnection<byte[], byte[]> connection) {
        Request waiter;
        while ((waiter = waiters.poll()) != null) {
            waiting.decrementAndGet();
            if (waiter.complete(connection)) {
                return Uni.createFrom().voidItem();
            }
        }
        return LettuceResult.toUni(() -> pool.release(connection)).replaceWithVoid();
    }

    private Uni<Void> releaseQuietly(StatefulRedisConnection<byte[], byte[]> connection) {
        return release(connection)
                .onFailure().invoke(failure -> LOGGER.warnf(failure,
                        "Failed to release pooled Redis connection back to the pool"))
                .onFailure().recoverWithNull();
    }

    private void returnToPool(StatefulRedisConnection<byte[], byte[]> connection) {
        releaseQuietly(connection).subscribe().with(ignored -> {
        });
    }

    /**
     * Runs {@code body} on a pooled connection, releasing it only once {@code body} truly
     * completes (item or failure) — never merely because the caller stopped waiting.
     */
    public <T> Uni<T> withPooled(Function<StatefulRedisConnection<byte[], byte[]>, Uni<T>> body) {
        return run(body, false);
    }

    /**
     * Runs {@code body} on a pooled connection that is scoped to the caller's subscription: the
     * caller's cancellation is propagated to {@code body}, and the connection is released on any
     * termination of {@code body} — item, failure or cancellation. This is the shape used by
     * {@code withConnection} and {@code withTransaction}.
     */
    public <T> Uni<T> withScoped(Function<StatefulRedisConnection<byte[], byte[]>, Uni<T>> body) {
        return run(body, true);
    }

    private <T> Uni<T> run(Function<StatefulRedisConnection<byte[], byte[]>, Uni<T>> body, boolean scoped) {
        return Uni.createFrom().emitter(emitter -> {
            Request request = new Request();
            AtomicReference<Cancellable> running = new AtomicReference<>();
            request.whenComplete((conn, failure) -> {
                if (failure != null) {
                    emitter.fail(failure);
                    return;
                }
                Cancellable subscription = Uni.createFrom().deferred(() -> body.apply(conn))
                        .onTermination().call(() -> releaseQuietly(conn))
                        .subscribe().with(emitter::complete, emitter::fail);
                if (scoped) {
                    running.set(subscription);
                }
            });
            emitter.onTermination(() -> {
                request.abandon();
                Cancellable subscription = running.get();
                if (subscription != null) {
                    subscription.cancel();
                }
            });
            request.start();
        });
    }

    public Uni<Void> close() {
        return LettuceResult.toUni(pool::closeAsync).replaceWithVoid();
    }

    public int getObjectCount() {
        return pool.getObjectCount();
    }

    public int getIdle() {
        return pool.getIdle();
    }

    private final class Request extends CompletableFuture<StatefulRedisConnection<byte[], byte[]>> {

        void start() {
            pool.acquire().whenComplete((conn, failure) -> {
                if (failure == null) {
                    if (!complete(conn)) {
                        returnToPool(conn);
                    }
                } else if (failure instanceof NoSuchElementException) {
                    enqueue();
                } else {
                    completeExceptionally(failure);
                }
            });
        }

        void abandon() {
            cancel(false);
            if (waiters.remove(this)) {
                waiting.decrementAndGet();
            }
        }

        private void enqueue() {
            if (waiting.incrementAndGet() > maxWaiting) {
                waiting.decrementAndGet();
                completeExceptionally(
                        new NoSuchElementException("Redis connection pool exhausted, too many waiting requests"));
                return;
            }
            waiters.add(this);
            if (isDone() && waiters.remove(this)) {
                waiting.decrementAndGet();
            }
        }

    }

}
