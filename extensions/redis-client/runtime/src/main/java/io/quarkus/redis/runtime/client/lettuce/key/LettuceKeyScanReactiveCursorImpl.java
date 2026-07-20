package io.quarkus.redis.runtime.client.lettuce.key;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.quarkus.redis.datasource.keys.ReactiveKeyScanCursor;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveKeyScanCursor}.
 * <p>
 * Wraps Lettuce's stateful {@code SCAN} pagination: each {@link #next()} call advances the
 * internal {@link ScanCursor}, returning the keys returned by that iteration. The optional
 * {@link io.lettuce.core.KeyScanArgs} is reapplied to every paginated call.
 */
public class LettuceKeyScanReactiveCursorImpl<K, V> implements ReactiveKeyScanCursor<K> {

    private final RedisAsyncCommands<K, V> async;
    private final io.lettuce.core.KeyScanArgs args;

    private ScanCursor cursor = ScanCursor.INITIAL;
    private boolean exhausted;

    public LettuceKeyScanReactiveCursorImpl(RedisAsyncCommands<K, V> async, io.lettuce.core.KeyScanArgs args) {
        this.async = async;
        this.args = args;
    }

    @Override
    public boolean hasNext() {
        return !exhausted;
    }

    @Override
    public Uni<Set<K>> next() {
        final ScanCursor current = cursor;
        final Supplier<CompletionStage<io.lettuce.core.KeyScanCursor<K>>> supplier;
        if (current == ScanCursor.INITIAL) {
            supplier = (args == null) ? async::scan : () -> async.scan(args);
        } else {
            supplier = (args == null) ? () -> async.scan(current) : () -> async.scan(current, args);
        }
        return LettuceResult.toUni(supplier)
                .invoke(result -> {
                    cursor = result;
                    if (result.isFinished()) {
                        exhausted = true;
                    }
                })
                .map(result -> new LinkedHashSet<>(result.getKeys()));
    }

    @Override
    public long cursorId() {
        if (cursor == ScanCursor.INITIAL || cursor.isFinished()) {
            return 0L;
        }
        return Long.parseUnsignedLong(cursor.getCursor());
    }

    @Override
    public Multi<K> toMulti() {
        return Multi.createBy().repeating()
                .uni(this::next)
                .whilst(set -> hasNext())
                .onItem().transformToMultiAndConcatenate(set -> Multi.createFrom().items(set.stream()));
    }
}
