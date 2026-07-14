package io.quarkus.redis.runtime.client.lettuce.hash;

import java.util.Map;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.quarkus.redis.datasource.hash.ReactiveHashScanCursor;
import io.quarkus.redis.runtime.client.lettuce.LettuceResult;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed {@link ReactiveHashScanCursor}. Drives HSCAN, carrying the server cursor across
 * {@link #next()} calls until it wraps back to the initial position.
 *
 * @param <F> the field type
 * @param <V> the value type
 */
public class LettuceHashScanCursor<F, V> implements ReactiveHashScanCursor<F, V> {

    private final RedisHashAsyncCommands<F, V> hash;
    private final F key;
    private final ScanArgs scanArgs;
    private ScanCursor cursor = ScanCursor.INITIAL;
    private boolean initial = true;

    public LettuceHashScanCursor(RedisHashAsyncCommands<F, V> hash, F key) {
        this(hash, key, new ScanArgs());
    }

    public LettuceHashScanCursor(RedisHashAsyncCommands<F, V> hash, F key, ScanArgs scanArgs) {
        this.hash = hash;
        this.key = key;
        this.scanArgs = scanArgs;
    }

    @Override
    public boolean hasNext() {
        return initial || !cursor.isFinished();
    }

    @Override
    public Uni<Map<F, V>> next() {
        initial = false;
        return LettuceResult.toUni(() -> hash.hscan(key, cursor, scanArgs))
                .invoke(mc -> this.cursor = mc)
                .map(mc -> mc.getMap());
    }

    @Override
    public Multi<Map.Entry<F, V>> toMulti() {
        return Multi.createBy().repeating().uni(this::next).whilst(m -> hasNext())
                .onItem().transformToMultiAndConcatenate(m -> Multi.createFrom().iterable(m.entrySet()));
    }

    @Override
    public long cursorId() {
        return Long.parseUnsignedLong(cursor.getCursor());
    }
}
