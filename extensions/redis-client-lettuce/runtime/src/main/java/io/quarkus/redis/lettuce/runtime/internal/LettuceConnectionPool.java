package io.quarkus.redis.lettuce.runtime.internal;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import io.vertx.core.Context;

/**
 * Bounded pool of {@code StatefulRedisConnection<byte[], byte[]>}, used for blocking commands
 * and scoped connections ({@code withConnection}/{@code withTransaction}) so that they never
 * occupy the shared multiplexed connection used by ordinary commands.
 * <p>
 * {@link BoundedAsyncPool#acquire()} fails immediately with a {@link NoSuchElementException}
 * once {@code maxTotal} connections are checked out — it has no waiting queue. This class adds
 * one, bounded by {@code maxWaiting}, mirroring the Vert.x backend's {@code max-pool-waiting}.
 * <p>
 * Pooled connections must come back in the state a fresh connection has, or the next borrower
 * inherits it. {@code MULTI}/{@code WATCH} are undone by the transaction paths; {@code SELECT} is
 * undone here: a borrower that changes the database calls {@link #markDirty(StatefulRedisConnection)},
 * and {@link #release(StatefulRedisConnection)} issues {@code SELECT <defaultDatabase>} before the
 * connection is handed on. If that reset fails, the connection stays marked and the reset is retried
 * before it is next handed out; a borrower is never given a connection on the wrong database.
 */
public final class LettuceConnectionPool {

    private static final Logger LOGGER = Logger.getLogger(LettuceConnectionPool.class);

    private final BoundedAsyncPool<StatefulRedisConnection<byte[], byte[]>> pool;
    private final int maxWaiting;
    /** Database a fresh connection is on ({@code RedisURI.getDatabase()}), restored on release. */
    private final int defaultDatabase;
    /** Connections whose selected database may differ from {@link #defaultDatabase}. */
    private final Set<StatefulRedisConnection<byte[], byte[]>> dirty = ConcurrentHashMap.newKeySet();

    /**
     * Serializes "connection available, else queue me" against "anyone queued, else return the
     * connection to the idle cache", so a release cannot slip between an acquire seeing the pool
     * exhausted and that acquire being queued (a lost wake-up). This relies on {@link BoundedAsyncPool}
     * deciding {@code acquire()} and adding to the idle cache in {@code release()} synchronously, which
     * holds only while {@code testOnAcquire}/{@code testOnRelease} stay off. Nothing that can run user
     * code — completing a request, subscribing a body — may happen while it is held.
     */
    private final Object lock = new Object();
    /** Guarded by {@link #lock}. Every queued request is live: not yet completed, failed or abandoned. */
    private final Deque<Request> waiters = new ArrayDeque<>();
    /** Guarded by {@link #lock}. */
    private boolean closed;

    /**
     * @param connector opens a new connection; the connection must start on {@code defaultDatabase}
     * @param maxPoolSize maximum number of connections
     * @param maxWaiting maximum number of requests queued once {@code maxPoolSize} are checked out
     * @param defaultDatabase database of a fresh connection, restored before a connection is reused
     *        after a borrower changed it
     */
    public LettuceConnectionPool(Supplier<CompletionStage<StatefulRedisConnection<byte[], byte[]>>> connector,
            int maxPoolSize, int maxWaiting, int defaultDatabase) {
        BoundedPoolConfig poolConfig = BoundedPoolConfig.builder()
                .maxTotal(maxPoolSize)
                .maxIdle(maxPoolSize)
                .build();
        this.pool = AsyncConnectionPoolSupport.createBoundedObjectPool(connector, poolConfig, false);
        this.maxWaiting = maxWaiting;
        this.defaultDatabase = defaultDatabase;
    }

    /**
     * Records that {@code connection} may no longer be on {@link #defaultDatabase}, so that it is
     * reset before anyone else uses it. Called by a borrower before it issues {@code SELECT}.
     */
    public void markDirty(StatefulRedisConnection<byte[], byte[]> connection) {
        dirty.add(connection);
    }

    /**
     * Whether {@code connection} is marked as possibly being on the wrong database.
     */
    public boolean isDirty(StatefulRedisConnection<byte[], byte[]> connection) {
        return dirty.contains(connection);
    }

    /**
     * Blocks until a connection is available or {@code timeout} elapses. On timeout (or interrupt)
     * the request is withdrawn from the queue and a connection that arrives just as the caller
     * gives up is returned to the pool rather than leaked.
     */
    public StatefulRedisConnection<byte[], byte[]> acquireBlocking(Duration timeout) {
        if (Context.isOnEventLoopThread()) {
            throw new IllegalStateException("acquireBlocking must not be called from an event loop thread");
        }
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
     * Releases a connection back to the pool, handing it straight to the next waiter if any. A
     * connection marked by {@link #markDirty(StatefulRedisConnection)} is first put back on
     * {@link #defaultDatabase}; if that fails it is handed on still marked, and the reset is
     * retried before the next borrower gets it.
     */
    public Uni<Void> release(StatefulRedisConnection<byte[], byte[]> connection) {
        if (!dirty.contains(connection)) {
            return handOver(connection);
        }
        return resetDatabase(connection)
                .onFailure().invoke(failure -> LOGGER.warnf(failure,
                        "Failed to reset pooled Redis connection to database %d on release, will retry before its next use",
                        defaultDatabase))
                .onFailure().recoverWithNull()
                .chain(() -> handOver(connection));
    }

    private Uni<Void> handOver(StatefulRedisConnection<byte[], byte[]> connection) {
        while (true) {
            Request waiter;
            synchronized (lock) {
                waiter = waiters.poll();
                if (waiter == null) {
                    return Uni.createFrom().completionStage(pool.release(connection));
                }
            }
            if (waiter.deliver(connection)) {
                return Uni.createFrom().voidItem();
            }
            // Abandoned between being polled and being completed: pick another.
        }
    }

    /**
     * Issues {@code SELECT defaultDatabase} and clears the dirty mark once Redis confirms it.
     * Lettuce records the confirmed database in its connection state, so a later reconnect
     * replays the default database rather than the one the borrower selected.
     */
    private Uni<Void> resetDatabase(StatefulRedisConnection<byte[], byte[]> connection) {
        return LettuceResult.toUni(() -> connection.async().select(defaultDatabase))
                .invoke(() -> dirty.remove(connection))
                .replaceWithVoid();
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

    /**
     * Closes the pool. Requests still queued for a connection fail with an
     * {@link IllegalStateException}, as do any made afterward.
     */
    public Uni<Void> close() {
        List<Request> queued;
        synchronized (lock) {
            closed = true;
            queued = new ArrayList<>(waiters);
            waiters.clear();
        }
        for (Request waiter : queued) {
            waiter.completeExceptionally(new IllegalStateException("Redis connection pool is closed"));
        }
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
            CompletableFuture<StatefulRedisConnection<byte[], byte[]>> acquired;
            synchronized (lock) {
                if (isDone()) {
                    return; // abandoned before we got here
                }
                if (closed) {
                    acquired = CompletableFuture.failedFuture(new IllegalStateException("Redis connection pool is closed"));
                } else {
                    acquired = pool.acquire();
                    if (isExhausted(acquired)) {
                        if (waiters.size() < maxWaiting) {
                            waiters.add(this);
                            return;
                        }
                        acquired = CompletableFuture.failedFuture(
                                new NoSuchElementException("Redis connection pool exhausted, too many waiting requests"));
                    }
                }
            }
            // Attached outside the lock: an already-completed future runs the callback inline.
            acquired.whenComplete((conn, failure) -> {
                if (failure == null) {
                    if (!deliver(conn)) {
                        returnToPool(conn);
                    }
                } else {
                    completeExceptionally(failure);
                }
            });
        }

        /**
         * Hands {@code conn} to this request. Returns {@code false} if the request was already
         * abandoned and {@code conn} is still the caller's to return. A connection still marked
         * dirty (its reset failed on release) is reset first; if that fails again the request
         * fails and the connection goes back to the pool, still marked, rather than being handed
         * out on the wrong database.
         */
        boolean deliver(StatefulRedisConnection<byte[], byte[]> conn) {
            if (!dirty.contains(conn)) {
                return complete(conn);
            }
            if (isDone()) {
                return false;
            }
            resetDatabase(conn).subscribe().with(
                    ignored -> {
                        if (!complete(conn)) {
                            returnToPool(conn);
                        }
                    },
                    failure -> {
                        completeExceptionally(failure);
                        returnToPool(conn);
                    });
            return true;
        }

        void abandon() {
            synchronized (lock) {
                waiters.remove(this);
            }
            cancel(false);
        }

        private static boolean isExhausted(CompletableFuture<?> acquired) {
            if (!acquired.isCompletedExceptionally()) {
                return false;
            }
            try {
                acquired.join();
                return false;
            } catch (CompletionException e) {
                return e.getCause() instanceof NoSuchElementException;
            }
        }

    }

}
