package io.quarkus.redis.runtime.client.lettuce.sortedset;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.List;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.quarkus.redis.datasource.sortedset.ReactiveZScanCursor;
import io.quarkus.redis.datasource.sortedset.ScoredValue;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed {@link ReactiveZScanCursor}. Drives ZSCAN, carrying the server cursor across
 * {@link #next()} calls until it wraps back to the initial position.
 *
 * @param <K> the key type
 * @param <V> the type of the scored member
 */
public class LettuceReactiveZScanCursorImpl<K, V> implements ReactiveZScanCursor<V> {

    private final RedisSortedSetAsyncCommands<K, V> sortedSet;
    private final K key;
    private final ScanArgs scanArgs;
    private ScanCursor cursor = ScanCursor.INITIAL;

    public LettuceReactiveZScanCursorImpl(RedisSortedSetAsyncCommands<K, V> sortedSet, K key) {
        this(sortedSet, key, new ScanArgs());
    }

    public LettuceReactiveZScanCursorImpl(RedisSortedSetAsyncCommands<K, V> sortedSet, K key, ScanArgs scanArgs) {
        nonNull(sortedSet, "sortedSet");
        nonNull(key, "key");
        nonNull(scanArgs, "scanArgs");
        this.sortedSet = sortedSet;
        this.key = key;
        this.scanArgs = scanArgs;
    }

    @Override
    public boolean hasNext() {
        return !cursor.isFinished();
    }

    @Override
    public Uni<List<ScoredValue<V>>> next() {
        // Reset cursor when finished to copy Vert.x. behavior.
        final ScanCursor current = cursor.isFinished() ? ScanCursor.INITIAL : cursor;
        return LettuceResult.toUni(() -> sortedSet.zscan(key, current, scanArgs))
                .invoke(sc -> this.cursor = sc)
                .map(ScoredValueScanCursor::getValues)
                .map(LettuceReactiveSortedSetCommandsImpl::toScoredValues);
    }

    @Override
    public long cursorId() {
        return Long.parseUnsignedLong(cursor.getCursor());
    }

    @Override
    public Multi<ScoredValue<V>> toMulti() {
        return Multi.createBy().repeating()
                .uni(this::next)
                .whilst(m -> hasNext())
                .onItem().transformToMultiAndConcatenate(list -> Multi.createFrom().iterable(list));
    }
}
