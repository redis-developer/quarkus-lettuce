package io.quarkus.redis.runtime.client.lettuce;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.quarkus.redis.runtime.datasource.Marshaller;

/**
 * Base class for Lettuce-backed reactive command group implementations.
 * <p>
 * Holds the async command handle and helper methods.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class AbstractLettuceCommands<K, V> {

    protected final RedisAsyncCommands<byte[], byte[]> async;
    protected final Type keyType;
    protected final Type valueType;
    protected final Marshaller marshaller;

    protected AbstractLettuceCommands(StatefulRedisConnection<byte[], byte[]> connection, Type keyType, Type valueType,
            Marshaller marshaller) {
        nonNull(connection, "connection");
        nonNull(keyType, "keyType");
        nonNull(valueType, "valueType");
        nonNull(marshaller, "marshaller");
        this.async = connection.async();
        this.keyType = keyType;
        this.valueType = valueType;
        this.marshaller = marshaller;
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

    protected <A, B> Map<byte[], byte[]> encodeMapWithNullableValues(Map<A, B> map) {
        Map<byte[], byte[]> encoded = new LinkedHashMap<>(map.size());
        for (Map.Entry<A, B> e : map.entrySet()) {
            byte[] value = marshaller.encode(e.getValue());
            encoded.put(marshaller.encode(nonNull(e.getKey(), "map key")),
                    value != null ? value : "null".getBytes(StandardCharsets.UTF_8));
        }
        return encoded;
    }

    protected <A, B> Map<byte[], byte[]> encodeMap(Map<A, B> map) {
        Map<byte[], byte[]> encoded = new LinkedHashMap<>(map.size());
        for (Map.Entry<A, B> e : map.entrySet()) {
            byte[] value = marshaller.encode(e.getValue());
            encoded.put(marshaller.encode(nonNull(e.getKey(), "map key")), nonNull(value, "value"));
        }
        return encoded;
    }

    public K decodeK(byte[] key) {
        return marshaller.decode(keyType, key);
    }

    public List<K> decodeListOfKeys(List<byte[]> list) {
        return list.stream().map(this::decodeK).toList();
    }

    public V decodeV(byte[] bytes) {
        return marshaller.decode(valueType, bytes);
    }

    public List<V> decodeListOfValue(List<byte[]> list) {
        return list.stream().map(this::decodeV).toList();
    }

    public <T> Map<T, V> decodeAsOrderedMap(T[] keys, List<KeyValue<byte[], byte[]>> results) {
        Map<T, V> map = new LinkedHashMap<>();
        for (int i = 0; i < keys.length; i++) {
            KeyValue<byte[], byte[]> kv = results.get(i);
            map.put(keys[i], kv.hasValue() ? decodeV(kv.getValue()) : null);
        }
        return map;
    }

    public String decodeString(byte[] bytes) {
        return marshaller.decode(String.class, bytes);
    }

}
