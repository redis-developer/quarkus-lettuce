package io.quarkus.redis.runtime.client.lettuce;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import io.smallrye.mutiny.Uni;

/**
 * Utility for adapting Lettuce {@link CompletionStage} results to Mutiny {@link Uni}
 * and to blocking calls.
 * <p>
 * Lettuce async commands return {@link io.lettuce.core.RedisFuture} which extends
 * {@link CompletionStage}. This class provides the bridge to Quarkus APIs without
 * exposing any Reactor types.
 */
public final class LettuceResult {

    private LettuceResult() {
        // Utility class
    }

    /**
     * Converts a {@link CompletionStage} to a Mutiny {@link Uni}.
     * <p>
     * The returned {@code Uni} subscribes lazily — the {@code CompletionStage} supplier
     * is invoked only when a subscriber requests it.
     *
     * @param <T> the result type
     * @param supplier a supplier of the {@link CompletionStage} (typically a Lettuce async command call)
     * @return a {@link Uni} that completes with the result of the {@link CompletionStage}
     */
    public static <T> Uni<T> toUni(java.util.function.Supplier<CompletionStage<T>> supplier) {
        return Uni.createFrom().completionStage(supplier);
    }

    /**
     * Blocks on a {@link CompletionStage} and returns the result.
     * <p>
     * This must only be called from a worker thread, never from an event loop thread.
     *
     * @param <T> the result type
     * @param stage the {@link CompletionStage} to block on
     * @param timeout the maximum time to wait
     * @return the result
     * @throws java.util.concurrent.CompletionException if the computation threw an exception
     */
    public static <T> T toBlocking(CompletionStage<T> stage, Duration timeout) {
        return Uni.createFrom().completionStage(stage)
                .await().atMost(timeout);
    }
}
