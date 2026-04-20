package io.quarkus.redis.runtime.client.lettuce;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link QuarkusRedisCodec}.
 */
class QuarkusRedisCodecTest {

    private final QuarkusRedisCodec<String, String> stringCodec = new QuarkusRedisCodec<>(String.class, String.class);

    @Test
    void encodeAndDecodeKey() {
        ByteBuffer encoded = stringCodec.encodeKey("mykey");
        String decoded = stringCodec.decodeKey(encoded);
        assertThat(decoded).isEqualTo("mykey");
    }

    @Test
    void encodeAndDecodeValue() {
        ByteBuffer encoded = stringCodec.encodeValue("myvalue");
        String decoded = stringCodec.decodeValue(encoded);
        assertThat(decoded).isEqualTo("myvalue");
    }

    @Test
    void encodeNullKeyShouldReturnEmptyBuffer() {
        ByteBuffer encoded = stringCodec.encodeKey(null);
        assertThat(encoded.remaining()).isZero();
    }

    @Test
    void encodeNullValueShouldReturnEmptyBuffer() {
        ByteBuffer encoded = stringCodec.encodeValue(null);
        assertThat(encoded.remaining()).isZero();
    }

    @Test
    void decodeNullKeyShouldReturnNull() {
        assertThat(stringCodec.decodeKey(null)).isNull();
    }

    @Test
    void decodeNullValueShouldReturnNull() {
        assertThat(stringCodec.decodeValue(null)).isNull();
    }

    @Test
    void decodeEmptyBufferShouldReturnNull() {
        assertThat(stringCodec.decodeKey(ByteBuffer.allocate(0))).isNull();
        assertThat(stringCodec.decodeValue(ByteBuffer.allocate(0))).isNull();
    }

    @Test
    void encodeAndDecodeUnicodeStrings() {
        String unicode = "こんにちは Redis 🎉";
        ByteBuffer encoded = stringCodec.encodeValue(unicode);
        String decoded = stringCodec.decodeValue(encoded);
        assertThat(decoded).isEqualTo(unicode);
    }

    @Test
    void integerCodecShouldWork() {
        QuarkusRedisCodec<String, Integer> intCodec = new QuarkusRedisCodec<>(String.class, Integer.class);
        ByteBuffer encoded = intCodec.encodeValue(42);
        Integer decoded = intCodec.decodeValue(encoded);
        assertThat(decoded).isEqualTo(42);
    }

    @Test
    void doubleCodecShouldWork() {
        QuarkusRedisCodec<String, Double> doubleCodec = new QuarkusRedisCodec<>(String.class, Double.class);
        ByteBuffer encoded = doubleCodec.encodeValue(3.14);
        Double decoded = doubleCodec.decodeValue(encoded);
        assertThat(decoded).isEqualTo(3.14);
    }

    @Test
    void keyAndValueTypesShouldBeAccessible() {
        assertThat(stringCodec.keyType()).isEqualTo(String.class);
        assertThat(stringCodec.valueType()).isEqualTo(String.class);
    }

    @Test
    void encodedBytesShouldMatchUtf8() {
        ByteBuffer encoded = stringCodec.encodeKey("test");
        byte[] expected = "test".getBytes(StandardCharsets.UTF_8);
        byte[] actual = new byte[encoded.remaining()];
        encoded.get(actual);
        assertThat(actual).isEqualTo(expected);
    }
}
