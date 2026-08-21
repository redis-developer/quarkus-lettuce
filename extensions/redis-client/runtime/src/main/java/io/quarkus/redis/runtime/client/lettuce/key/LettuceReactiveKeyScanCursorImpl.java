package io.quarkus.redis.runtime.client.lettuce.key;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.LinkedHashSet;
import java.util.Set;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.quarkus.redis.datasource.keys.ReactiveKeyScanCursor;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed {@link ReactiveKeyScanCursor}. Drives SCAN, carrying the server cursor across
 * {@link #next()} calls until it wraps back to the initial position.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LettuceReactiveKeyScanCursorImpl<K, V> implements ReactiveKeyScanCursor<K> {

    private final RedisAsyncCommands<K, V> async;
    private final KeyScanArgs keyScanArgs;
    private ScanCursor cursor = ScanCursor.INITIAL;

    public LettuceReactiveKeyScanCursorImpl(RedisAsyncCommands<K, V> async) {
        this(async, new KeyScanArgs());
    }

    public LettuceReactiveKeyScanCursorImpl(RedisAsyncCommands<K, V> async, KeyScanArgs keyScanArgs) {
        nonNull(async, "async");
        nonNull(keyScanArgs, "args");
        this.async = async;
        this.keyScanArgs = keyScanArgs;
    }

    @Override
    public boolean hasNext() {
        return !cursor.isFinished();
    }

    @Override
    public Uni<Set<K>> next() {
        // Reset cursor when finished to copy Vert.x. behavior.
        final ScanCursor current = cursor.isFinished() ? ScanCursor.INITIAL : cursor;
        return LettuceResult.toUni(() -> async.scan(current, keyScanArgs))
                .invoke(kc -> this.cursor = kc)
                .map(kc -> new LinkedHashSet<>(kc.getKeys()));
    }

    @Override
    public long cursorId() {
        return Long.parseUnsignedLong(cursor.getCursor());
    }

    @Override
    public Multi<K> toMulti() {
        return Multi.createBy().repeating()
                .uni(this::next)
                .whilst(set -> hasNext())
                .onItem().transformToMultiAndConcatenate(set -> Multi.createFrom().iterable(set));
    }
}
