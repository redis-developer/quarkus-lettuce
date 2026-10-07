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

import io.lettuce.core.support.AsyncObjectFactory;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.core.Context;

/**
 * Bounded pool of {@link LettuceConnection}s (to a standalone server or to a cluster, see
 * {@link LettuceConnectionFactory}), used for blocking commands and scoped connections
 * ({@code withConnection}/{@code withTransaction}) so that they never occupy the shared multiplexed
 * connection used by ordinary commands.
 * <p>
 * {@link BoundedAsyncPool#acquire()} fails immediately with a {@link NoSuchElementException}
 * once {@code maxTotal} connections are checked out — it has no waiting queue. This class adds
 * one, bounded by {@code maxWaiting}, mirroring the Vert.x backend's {@code max-pool-waiting}.
 * <p>
 * Pooled connections must come back in the state a fresh connection has, or the next borrower
 * inherits it. {@code MULTI}/{@code WATCH} are undone by the transaction paths; {@code SELECT} is
 * undone here: a borrower that changes the database calls {@link #markDirty(LettuceConnection)},
 * and {@link #release(LettuceConnection)} issues {@code SELECT <defaultDatabase>} before the
 * connection is handed on. If that reset fails, the connection stays marked and the reset is retried
 * before it is next handed out; a borrower is never given a connection on the wrong database.
 * A cluster has a single database, so cluster connections are never marked: a {@code SELECT} is
 * rejected by the server and leaves nothing to reset.
 */
public final class LettuceConnectionPool {

    private static final Logger LOGGER = Logger.getLogger(LettuceConnectionPool.class);

    private final BoundedAsyncPool<LettuceConnection> pool;
    private final int maxWaiting;
    /** Database a fresh connection is on ({@code RedisURI.getDatabase()}), restored on release. */
    private final int defaultDatabase;
    /** Connections whose selected database may differ from {@link #defaultDatabase}. */
    private final Set<LettuceConnection> dirty = ConcurrentHashMap.newKeySet();

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
    public LettuceConnectionPool(Supplier<CompletionStage<LettuceConnection>> connector,
            int maxPoolSize, int maxWaiting, int defaultDatabase) {
        BoundedPoolConfig poolConfig = BoundedPoolConfig.builder()
                .maxTotal(maxPoolSize)
                .maxIdle(maxPoolSize)
                .build();
        this.pool = new BoundedAsyncPool<>(new PooledConnectionFactory(connector), poolConfig);
        this.maxWaiting = maxWaiting;
        this.defaultDatabase = defaultDatabase;
    }

    /**
     * Records that {@code connection} may no longer be on {@link #defaultDatabase}, so that it is
     * reset before anyone else uses it. Called by a borrower before it issues {@code SELECT}. A
     * cluster connection is never marked: a cluster has a single database, so the {@code SELECT}
     * the borrower is about to issue is rejected and leaves nothing to reset.
     */
    public void markDirty(LettuceConnection connection) {
        if (!connection.isCluster()) {
            dirty.add(connection);
        }
    }

    /**
     * Whether {@code connection} is marked as possibly being on the wrong database.
     */
    public boolean isDirty(LettuceConnection connection) {
        return dirty.contains(connection);
    }

    /**
     * Blocks until a connection is available or {@code timeout} elapses. On timeout (or interrupt)
     * the request is withdrawn from the queue and a connection that arrives just as the caller
     * gives up is returned to the pool rather than leaked.
     */
    public LettuceConnection acquireBlocking(Duration timeout) {
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
     * connection marked by {@link #markDirty(LettuceConnection)} is first put back on
     * {@link #defaultDatabase}; if that fails it is handed on still marked, and the reset is
     * retried before the next borrower gets it.
     */
    public Uni<Void> release(LettuceConnection connection) {
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

    private Uni<Void> handOver(LettuceConnection connection) {
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
    private Uni<Void> resetDatabase(LettuceConnection connection) {
        return LettuceResult.toUni(() -> connection.select(defaultDatabase))
                .invoke(() -> dirty.remove(connection))
                .replaceWithVoid();
    }

    private void redrive() {
        Request next;
        synchronized (lock) {
            next = waiters.poll();
        }
        if (next != null) {
            next.start();
        }
    }

    private Uni<Void> releaseQuietly(LettuceConnection connection) {
        return release(connection)
                .onFailure().invoke(failure -> LOGGER.warnf(failure,
                        "Failed to release pooled Redis connection back to the pool"))
                .onFailure().recoverWithNull();
    }

    private void returnToPool(LettuceConnection connection) {
        releaseQuietly(connection).subscribe().with(ignored -> {
        });
    }

    /**
     * Runs {@code body} on a pooled connection, releasing it only once {@code body} truly
     * completes (item or failure) — never merely because the caller stopped waiting.
     */
    public <T> Uni<T> withPooled(Function<LettuceConnection, Uni<T>> body) {
        return run(body, false);
    }

    /**
     * Runs {@code body} on a pooled connection that is scoped to the caller's subscription: the
     * caller's cancellation is propagated to {@code body}, and the connection is released on any
     * termination of {@code body} — item, failure or cancellation. This is the shape used by
     * {@code withConnection} and {@code withTransaction}.
     */
    public <T> Uni<T> withScoped(Function<LettuceConnection, Uni<T>> body) {
        return run(body, true);
    }

    private <T> Uni<T> run(Function<LettuceConnection, Uni<T>> body, boolean scoped) {
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

    private final class Request extends CompletableFuture<LettuceConnection> {

        void start() {
            CompletableFuture<LettuceConnection> acquired = null;
            RuntimeException rejection = null;
            synchronized (lock) {
                if (isDone()) {
                    return; // abandoned before we got here
                }
                if (closed) {
                    rejection = new IllegalStateException("Redis connection pool is closed");
                } else {
                    acquired = pool.acquire();
                    if (isExhausted(acquired)) {
                        if (waiters.size() < maxWaiting) {
                            waiters.add(this);
                            return;
                        }
                        rejection = new NoSuchElementException("Redis connection pool exhausted, too many waiting requests");
                    }
                }
            }
            if (rejection != null) {
                completeExceptionally(rejection);
                return;
            }
            acquired.whenComplete((conn, failure) -> {
                if (failure == null) {
                    if (!deliver(conn)) {
                        returnToPool(conn);
                    }
                } else {
                    completeExceptionally(failure);
                    redrive();
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
        boolean deliver(LettuceConnection conn) {
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

    /**
     * Opens, validates and closes the pooled connections: the equivalent of the factory Lettuce's own
     * {@code AsyncConnectionPoolSupport} uses, for a {@link LettuceConnection} rather than a Lettuce connection.
     */
    private static final class PooledConnectionFactory implements AsyncObjectFactory<LettuceConnection> {

        private final Supplier<CompletionStage<LettuceConnection>> connector;

        PooledConnectionFactory(Supplier<CompletionStage<LettuceConnection>> connector) {
            this.connector = connector;
        }

        @Override
        public CompletableFuture<LettuceConnection> create() {
            return connector.get().toCompletableFuture();
        }

        @Override
        public CompletableFuture<Void> destroy(LettuceConnection connection) {
            return connection.closeAsync();
        }

        @Override
        public CompletableFuture<Boolean> validate(LettuceConnection connection) {
            return CompletableFuture.completedFuture(connection.isOpen());
        }

    }

}
