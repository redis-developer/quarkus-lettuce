package io.quarkus.redis.runtime.client.lettuce;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

/**
 * Base class for Lettuce-backed reactive command group implementations.
 * <p>
 * Holds the async command handle and decoding methods.
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

    public static boolean isOk(String response) {
        return "OK".equals(response);
    }

    public static <T> List<T> orEmpty(List<T> list) {
        if (list == null) {
            return Collections.emptyList();
        }
        return list;
    }

    public static Boolean asBoolean(Long added) {
        return added != null && added == 1L;
    }

    public static Long orZero(Long value) {
        if (value == null) {
            return 0L;
        }
        return value;
    }

    public static boolean isWholeSeconds(Duration duration) {
        return duration.getNano() == 0;
    }

    public static double toFractionalSeconds(Duration duration) {
        return duration.toMillis() / 1_000.0d;
    }
}
