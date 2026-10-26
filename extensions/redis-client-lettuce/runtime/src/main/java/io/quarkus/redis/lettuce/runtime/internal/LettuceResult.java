package io.quarkus.redis.lettuce.runtime.internal;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;

/**
 * Utility for awaiting Lettuce {@link CompletionStage} results from blocking callers.
 * <p>
 * Lettuce async commands return {@link io.lettuce.core.RedisFuture} which extends {@link CompletionStage}; the
 * reactive callers adapt them with {@code Uni.createFrom().completionStage(...)} directly.
 */
public final class LettuceResult {

    private LettuceResult() {
        // Utility class
    }

    /**
     * Blocks on a {@link CompletionStage} and returns the result.
     * <p>
     * Must only be called from a worker thread, never from an event loop thread; this is enforced
     * via {@link Context#isOnEventLoopThread()}.
     *
     * @param <T> the result type
     * @param stage the {@link CompletionStage} to block on
     * @param timeout the maximum time to wait
     * @return the result
     * @throws IllegalStateException if invoked on an event loop thread
     * @throws java.util.concurrent.CompletionException if the computation threw an exception
     */
    public static <T> T toBlocking(CompletionStage<T> stage, Duration timeout) {
        if (Context.isOnEventLoopThread()) {
            throw new IllegalStateException(
                    "LettuceResult.toBlocking must not be called from an event loop thread");
        }
        return Uni.createFrom().completionStage(stage)
                .await().atMost(timeout);
    }
}
