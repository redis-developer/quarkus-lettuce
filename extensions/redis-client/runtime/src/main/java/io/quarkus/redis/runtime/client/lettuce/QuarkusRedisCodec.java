package io.quarkus.redis.runtime.client.lettuce;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;

import io.lettuce.core.codec.RedisCodec;
import io.quarkus.redis.datasource.codecs.Codec;
import io.quarkus.redis.datasource.codecs.Codecs;

/**
 * Adapts Quarkus {@link Codec} (byte[]-based) to Lettuce {@link RedisCodec} (ByteBuffer-based).
 * <p>
 * This allows Lettuce commands to use the same serialization logic as the existing Quarkus
 * Redis extension, including custom user-provided codecs registered via CDI.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class QuarkusRedisCodec<K, V> implements RedisCodec<K, V> {

    private final Codec keyCodec;
    private final Codec valueCodec;
    private final Type keyType;
    private final Type valueType;

    public QuarkusRedisCodec(Type keyType, Type valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyCodec = Codecs.getDefaultCodecFor(keyType);
        this.valueCodec = Codecs.getDefaultCodecFor(valueType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public K decodeKey(ByteBuffer bytes) {
        // Only a nil reply (null buffer) decodes to null. An empty but non-null buffer is a stored
        // empty string, a valid Redis key/value, and must round-trip as such rather than as null.
        if (bytes == null) {
            return null;
        }
        return (K) keyCodec.decode(toBytes(bytes));
    }

    @Override
    @SuppressWarnings("unchecked")
    public V decodeValue(ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        return (V) valueCodec.decode(toBytes(bytes));
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        if (key == null) {
            return ByteBuffer.allocate(0);
        }
        return ByteBuffer.wrap(keyCodec.encode(key));
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        if (value == null) {
            return ByteBuffer.allocate(0);
        }
        return ByteBuffer.wrap(valueCodec.encode(value));
    }

    /**
     * @return the key type this codec handles
     */
    public Type keyType() {
        return keyType;
    }

    /**
     * @return the value type this codec handles
     */
    public Type valueType() {
        return valueType;
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
