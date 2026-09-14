package io.quarkus.redis.lettuce.runtime.internal.datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.quarkus.redis.datasource.transactions.OptimisticLockingTransactionResult;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.lettuce.runtime.internal.LettuceCommand;
import io.quarkus.redis.runtime.datasource.OptimisticLockingTransactionResultImpl;
import io.quarkus.redis.runtime.datasource.TransactionResultImpl;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce equivalent of {@link io.quarkus.redis.runtime.datasource.TransactionHolder}.
 * <p>
 * Unlike the Vert.x backend — which receives a {@code QUEUED} reply per command issued
 * between {@code MULTI} and {@code EXEC} — Lettuce does not complete a command's
 * {@link RedisFuture} until {@code EXEC} runs. So instead of awaiting each command, this
 * holder captures the per-command {@link RedisFuture} (in enqueue order) together with a
 * result mapper, and reconstructs the typed {@link TransactionResult} once {@code EXEC} has
 * completed and all captured futures have settled.
 * <p>
 * Each entry is a {@link LettuceCommand} carrying the same mapper the non-transactional
 * implementation applies via {@link LettuceCommand#toUni()}, so
 * {@code TransactionResult.get(index)} returns the same Java type the Vert.x backend produces.
 */
public class LettuceTransactionHolder {

    private final List<RedisFuture<?>> futures = new ArrayList<>();
    private final List<Function<Object, Object>> mappers = new ArrayList<>();
    private volatile boolean discarded = false;

    /**
     * Issues a command into the open {@code MULTI} block and records it for later assembly.
     * <p>
     * The command's {@link LettuceCommand#call() call} supplier is invoked eagerly so the command
     * is enqueued on the pinned connection in call order, which matches the order of the
     * {@code EXEC} reply. A supplier that throws instead of issuing a command — the command
     * implementations defer some argument validations that way — fails the returned {@link Uni}
     * rather than the call, as the Vert.x backend and the non-transactional path do, and records
     * no entry. The command's {@link LettuceCommand#mapper() mapper} is applied to the raw
     * {@code EXEC} reply when the {@link TransactionResult} is assembled.
     *
     * @param command the command to issue, carrying its call and result mapper
     * @param <T> the raw Lettuce result type
     * @param <R> the Quarkus result type recorded in the {@link TransactionResult}
     * @return a {@link Uni} completing immediately — the result is only available after {@code EXEC}
     */
    @SuppressWarnings("unchecked")
    public <T, R> Uni<Void> enqueue(LettuceCommand<T, R> command) {
        RedisFuture<T> future;
        try {
            future = command.call().get();
        } catch (RuntimeException e) {
            return Uni.createFrom().failure(e);
        }
        futures.add(future);
        mappers.add((Function<Object, Object>) command.mapper());
        return Uni.createFrom().voidItem();
    }

    public void discard() {
        discarded = true;
    }

    public boolean discarded() {
        return discarded;
    }

    public int size() {
        return futures.size();
    }

    /**
     * Builds a {@link TransactionResult} once {@code EXEC} has completed. Awaits all captured
     * futures (they settle when {@code EXEC} runs) before decoding each entry.
     */
    public Uni<TransactionResult> toResult() {
        return awaitAll().map(ignored -> {
            boolean[] hasErrors = { false };
            List<Object> results = collect(hasErrors);
            return new TransactionResultImpl(discarded, hasErrors[0], results);
        });
    }

    /**
     * Builds an {@link OptimisticLockingTransactionResult}, attaching the pre-transaction result.
     */
    public <I> Uni<OptimisticLockingTransactionResult<I>> toOptimisticLockingResult(I input) {
        return awaitAll().map(ignored -> {
            boolean[] hasErrors = { false };
            List<Object> results = collect(hasErrors);
            return new OptimisticLockingTransactionResultImpl<>(discarded, hasErrors[0], input, results);
        });
    }

    private Uni<Void> awaitAll() {
        if (futures.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        CompletableFuture<?>[] settled = new CompletableFuture<?>[futures.size()];
        for (int i = 0; i < futures.size(); i++) {
            // handle() yields an already-resolvable stage regardless of success/failure, so
            // allOf waits for every command to settle without short-circuiting on the first error.
            settled[i] = futures.get(i).toCompletableFuture().handle((v, t) -> null);
        }
        return Uni.createFrom().completionStage(CompletableFuture.allOf(settled)).replaceWithVoid();
    }

    private List<Object> collect(boolean[] hasErrors) {
        List<Object> results = new ArrayList<>(futures.size());
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<?> future = futures.get(i).toCompletableFuture();
            try {
                Object raw = future.getNow(null);
                results.add(mappers.get(i).apply(raw));
            } catch (CompletionException e) {
                hasErrors[0] = true;
                results.add(e.getCause() != null ? e.getCause() : e);
            } catch (CancellationException e) {
                hasErrors[0] = true;
                results.add(e);
            }
        }
        return results;
    }

}
