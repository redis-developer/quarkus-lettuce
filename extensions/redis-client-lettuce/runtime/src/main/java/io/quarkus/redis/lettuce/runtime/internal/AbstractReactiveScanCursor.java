package io.quarkus.redis.lettuce.runtime.internal;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.quarkus.redis.datasource.ReactiveCursor;

public abstract class AbstractReactiveScanCursor<T> implements ReactiveCursor<T> {

    protected final RedisClusterAsyncCommands<byte[], byte[]> async;
    protected final ScanArgs scanArgs;
    protected ScanCursor cursor = ScanCursor.INITIAL;

    protected AbstractReactiveScanCursor(RedisClusterAsyncCommands<byte[], byte[]> async, ScanArgs scanArgs) {
        nonNull(async, "async");
        nonNull(scanArgs, "scanArgs");
        this.async = async;
        this.scanArgs = scanArgs;
    }

    @Override
    public boolean hasNext() {
        return !cursor.isFinished();
    }

    @Override
    public long cursorId() {
        return Long.parseUnsignedLong(cursor.getCursor());
    }

}
