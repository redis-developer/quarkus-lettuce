package io.quarkus.redis.lettuce.runtime.internal;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.CommandArgs;

public final class BlockingCommandArgs<K, V> extends CommandArgs<K, V> {

    public BlockingCommandArgs(RedisCodec<K, V> codec) {
        super(codec);
    }

}
