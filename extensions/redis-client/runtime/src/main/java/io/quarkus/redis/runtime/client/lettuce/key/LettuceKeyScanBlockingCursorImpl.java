package io.quarkus.redis.runtime.client.lettuce.key;

import java.time.Duration;
import java.util.Set;

import io.quarkus.redis.datasource.keys.KeyScanCursor;
import io.quarkus.redis.datasource.keys.ReactiveKeyScanCursor;

/**
 * Blocking wrapper around {@link LettuceKeyScanReactiveCursorImpl}. Each method awaits the
 * reactive cursor's emission for the configured {@code timeout}. Must not be invoked from
 * an event loop thread.
 */
public class LettuceKeyScanBlockingCursorImpl<K> implements KeyScanCursor<K> {

    private final ReactiveKeyScanCursor<K> reactive;
    private final Duration timeout;

    public LettuceKeyScanBlockingCursorImpl(ReactiveKeyScanCursor<K> reactive, Duration timeout) {
        this.reactive = reactive;
        this.timeout = timeout;
    }

    @Override
    public boolean hasNext() {
        return reactive.hasNext();
    }

    @Override
    public Set<K> next() {
        return reactive.next().await().atMost(timeout);
    }

    @Override
    public long cursorId() {
        return reactive.cursorId();
    }

    @Override
    public Iterable<K> toIterable() {
        return reactive.toMulti().subscribe().asIterable();
    }
}
