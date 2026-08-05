package io.quarkus.redis.runtime.client.lettuce.set;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.List;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ValueScanCursor;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.quarkus.redis.datasource.set.ReactiveSScanCursor;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed {@link ReactiveSScanCursor}. Drives SSCAN, carrying the server cursor across
 * {@link #next()} calls until it wraps back to the initial position.
 *
 * @param <K> the key type
 * @param <V> the member type
 */
public class LettuceReactiveSScanCursorImpl<K, V> implements ReactiveSScanCursor<V> {

    private final RedisSetAsyncCommands<K, V> set;
    private final K key;
    private final ScanArgs scanArgs;
    private ScanCursor cursor = ScanCursor.INITIAL;

    public LettuceReactiveSScanCursorImpl(RedisSetAsyncCommands<K, V> set, K key) {
        this(set, key, new ScanArgs());
    }

    public LettuceReactiveSScanCursorImpl(RedisSetAsyncCommands<K, V> set, K key, ScanArgs scanArgs) {
        nonNull(set, "set");
        nonNull(key, "key");
        nonNull(scanArgs, "scanArgs");
        this.set = set;
        this.key = key;
        this.scanArgs = scanArgs;
    }

    @Override
    public boolean hasNext() {
        return !cursor.isFinished();
    }

    @Override
    public Uni<List<V>> next() {
        final ScanCursor current = cursor;
        return LettuceResult.toUni(() -> set.sscan(key, current, scanArgs))
                .invoke(vc -> this.cursor = vc)
                .map(ValueScanCursor::getValues);
    }

    @Override
    public long cursorId() {
        return Long.parseUnsignedLong(cursor.getCursor());
    }

    @Override
    public Multi<V> toMulti() {
        return Multi.createBy().repeating()
                .uni(this::next)
                .whilst(m -> hasNext())
                .onItem().transformToMultiAndConcatenate(list -> Multi.createFrom().iterable(list));
    }
}
