package io.quarkus.redis.lettuce.runtime.internal;

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;

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
    private final Queue<UniEmitter<? super StatefulRedisConnection<byte[], byte[]>>> waiters = new ConcurrentLinkedQueue<>();
    private final AtomicInteger waiting = new AtomicInteger();

    public LettuceConnectionPool(Supplier<CompletionStage<StatefulRedisConnection<byte[], byte[]>>> connector,
            int maxPoolSize, int maxWaiting) {
        BoundedPoolConfig poolConfig = BoundedPoolConfig.builder()
                .maxTotal(maxPoolSize)
                .maxIdle(maxPoolSize)
                .minIdle(0)
                .build();
        this.pool = AsyncConnectionPoolSupport.createBoundedObjectPool(connector, poolConfig, false);
        this.maxWaiting = maxWaiting;
    }

    /**
     * Acquires a connection, queueing the caller when the pool is exhausted (up to {@code maxWaiting}
     * callers) instead of failing immediately as {@link BoundedAsyncPool#acquire()} does.
     */
    public Uni<StatefulRedisConnection<byte[], byte[]>> acquire() {
        return Uni.createFrom().<StatefulRedisConnection<byte[], byte[]>> emitter(this::doAcquire);
    }

    private void doAcquire(UniEmitter<? super StatefulRedisConnection<byte[], byte[]>> emitter) {
        pool.acquire().whenComplete((conn, failure) -> {
            if (failure == null) {
                emitter.complete(conn);
            } else if (failure instanceof NoSuchElementException) {
                enqueue(emitter);
            } else {
                emitter.fail(failure);
            }
        });
    }

    private void enqueue(UniEmitter<? super StatefulRedisConnection<byte[], byte[]>> emitter) {
        if (waiting.incrementAndGet() > maxWaiting) {
            waiting.decrementAndGet();
            emitter.fail(new NoSuchElementException("Redis connection pool exhausted, too many waiting requests"));
            return;
        }
        waiters.add(emitter);
        emitter.onTermination(() -> {
            if (waiters.remove(emitter)) {
                waiting.decrementAndGet();
            }
        });
    }

    /**
     * Releases a connection back to the pool, handing it straight to the next waiter if any.
     */
    public Uni<Void> release(StatefulRedisConnection<byte[], byte[]> connection) {
        UniEmitter<? super StatefulRedisConnection<byte[], byte[]>> waiter = waiters.poll();
        if (waiter != null) {
            waiting.decrementAndGet();
            waiter.complete(connection);
            return Uni.createFrom().voidItem();
        }
        return LettuceResult.toUni(() -> pool.release(connection)).replaceWithVoid();
    }

    /**
     * Runs {@code body} on a pooled connection, releasing it only once {@code body} truly
     * completes (item or failure) — never merely because the caller stopped waiting.
     * <p>
     * {@code body} is invoked lazily inside a deferred {@link Uni} so that an exception thrown
     * synchronously by {@code body.apply(conn)} (e.g. eager argument validation) is routed to
     * the failure path and the connection is released rather than leaked.
     */
    public <T> Uni<T> withPooled(Function<StatefulRedisConnection<byte[], byte[]>, Uni<T>> body) {
        return acquire()
                .onItem().transformToUni(conn -> {
                    CompletableFuture<T> result = new CompletableFuture<>();
                    Uni.createFrom().deferred(() -> body.apply(conn)).subscribe().with(
                            item -> releaseThenComplete(conn, result, item, null),
                            failure -> releaseThenComplete(conn, result, null, failure));
                    return Uni.createFrom().completionStage(result);
                });
    }

    private <T> void releaseThenComplete(StatefulRedisConnection<byte[], byte[]> conn, CompletableFuture<T> result,
            T item, Throwable failure) {
        release(conn).subscribe().with(
                ignored -> complete(result, item, failure),
                releaseFailure -> {
                    LOGGER.warnf(releaseFailure, "Failed to release pooled Redis connection back to the pool");
                    complete(result, item, failure);
                });
    }

    private static <T> void complete(CompletableFuture<T> result, T item, Throwable failure) {
        if (failure != null) {
            result.completeExceptionally(failure);
        } else {
            result.complete(item);
        }
    }

    public Uni<Void> close() {
        return LettuceResult.toUni(pool::closeAsync).replaceWithVoid();
    }

    /**
     * The number of connections currently checked out of or idle in the pool.
     */
    public int getObjectCount() {
        return pool.getObjectCount();
    }

    /**
     * The number of idle (available) connections in the pool.
     */
    public int getIdle() {
        return pool.getIdle();
    }

}
