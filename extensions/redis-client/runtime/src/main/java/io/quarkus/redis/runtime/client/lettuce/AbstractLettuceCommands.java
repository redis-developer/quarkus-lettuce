package io.quarkus.redis.runtime.client.lettuce;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

/**
 * Base class for Lettuce-backed reactive command group implementations.
 * <p>
 * Holds the async command handle and provides shared access to the
 * {@link LettuceConverterRegistry} for argument/result conversion.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class AbstractLettuceCommands<K, V> {

    protected final RedisAsyncCommands<K, V> async;

    protected AbstractLettuceCommands(StatefulRedisConnection<K, V> connection) {
        if (connection == null) {
            throw new IllegalArgumentException("`connection` must not be null");
        }
        this.async = connection.async();
    }
}
