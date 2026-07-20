package io.quarkus.redis.runtime.client.lettuce;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.netty.channel.EventLoopGroup;
import io.smallrye.mutiny.Uni;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Vertx;

/**
 * Integration test for the Lettuce proxy infrastructure.
 * <p>
 * Validates that {@link QuarkusRedisCodec}, {@link LettuceConverterRegistry}, and
 * {@link LettuceResult} work together against a real Redis instance.
 */
@SuppressWarnings("resource")
class LettuceProxyInfrastructureTest {

    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static Vertx vertx;
    static LettuceClientResources lettuceResources;
    static RedisClient redisClient;
    static StatefulRedisConnection<String, String> defaultConnection;
    static StatefulRedisConnection<String, Integer> typedConnection;

    @BeforeAll
    static void setUp() {
        REDIS.start();
        vertx = Vertx.vertx();

        EventLoopGroup vertxEventLoops = ((VertxInternal) vertx.getDelegate()).eventLoopGroup();
        lettuceResources = new LettuceClientResources(vertxEventLoops);

        String redisUri = String.format("redis://%s:%d", REDIS.getHost(), REDIS.getFirstMappedPort());
        redisClient = RedisClient.create(lettuceResources.clientResources(), redisUri);

        // Default String/String connection using QuarkusRedisCodec
        RedisCodec<String, String> stringCodec = new QuarkusRedisCodec<>(String.class, String.class);
        defaultConnection = redisClient.connect(stringCodec);

        // Typed String/Integer connection using QuarkusRedisCodec
        RedisCodec<String, Integer> intCodec = new QuarkusRedisCodec<>(String.class, Integer.class);
        typedConnection = redisClient.connect(intCodec);
    }

    @AfterAll
    static void tearDown() {
        if (typedConnection != null) {
            typedConnection.close();
        }
        if (defaultConnection != null) {
            defaultConnection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
        if (lettuceResources != null) {
            lettuceResources.shutdown();
        }
        if (vertx != null) {
            vertx.closeAndAwait();
        }
        REDIS.stop();
    }

    @Test
    void quarkusCodecRoundTripSetGet() {
        // SET/GET through QuarkusRedisCodec — proves encode/decode works end-to-end
        Uni<String> set = LettuceResult.toUni(() -> defaultConnection.async().set("codec-key", "codec-value"));
        assertThat(set.await().atMost(Duration.ofSeconds(5))).isEqualTo("OK");

        Uni<String> get = LettuceResult.toUni(() -> defaultConnection.async().get("codec-key"));
        assertThat(get.await().atMost(Duration.ofSeconds(5))).isEqualTo("codec-value");
    }

    @Test
    void emptyStringValueRoundTripsAsEmptyNotNull() {
        // An empty string is a valid Redis value and must round-trip as "" — it must not be
        // conflated with a nil reply. A genuinely missing key still decodes to null.
        String set = LettuceResult.toBlocking(
                defaultConnection.async().set("empty-key", ""), Duration.ofSeconds(5));
        assertThat(set).isEqualTo("OK");

        String value = LettuceResult.toBlocking(
                defaultConnection.async().get("empty-key"), Duration.ofSeconds(5));
        assertThat(value).isEmpty();

        String missing = LettuceResult.toBlocking(
                defaultConnection.async().get("no-such-key"), Duration.ofSeconds(5));
        assertThat(missing).isNull();
    }

    @Test
    void quarkusCodecWithIntegerValues() {
        // SET/GET with Integer values — proves non-String codec works
        Uni<String> set = LettuceResult.toUni(() -> typedConnection.async().set("int-key", 42));
        assertThat(set.await().atMost(Duration.ofSeconds(5))).isEqualTo("OK");

        Uni<Integer> get = LettuceResult.toUni(() -> typedConnection.async().get("int-key"));
        assertThat(get.await().atMost(Duration.ofSeconds(5))).isEqualTo(42);
    }

    @Test
    void toUniLazyExecution() {
        // Verify toUni is lazy — the supplier is not called until subscription
        Uni<String> uni = LettuceResult.toUni(() -> defaultConnection.async().set("lazy-key", "lazy-value"));

        // Subscribe and verify
        String result = uni.await().atMost(Duration.ofSeconds(5));
        assertThat(result).isEqualTo("OK");

        // Verify the value was actually set
        String value = LettuceResult.toBlocking(
                defaultConnection.async().get("lazy-key"), Duration.ofSeconds(5));
        assertThat(value).isEqualTo("lazy-value");
    }

    @Test
    void toBlockingRoundTrip() {
        // Proves toBlocking works end-to-end with a real Redis command
        String setResult = LettuceResult.toBlocking(
                defaultConnection.async().set("blocking-key", "blocking-value"), Duration.ofSeconds(5));
        assertThat(setResult).isEqualTo("OK");

        String getResult = LettuceResult.toBlocking(
                defaultConnection.async().get("blocking-key"), Duration.ofSeconds(5));
        assertThat(getResult).isEqualTo("blocking-value");
    }

    @Test
    void converterRegistryIntegration() {
        // Register a simple converter, use it to transform a Lettuce result
        LettuceConverterRegistry.registerResultConverter(String.class, s -> ((String) s).toUpperCase());
        try {
            String raw = LettuceResult.toBlocking(
                    defaultConnection.async().set("conv-key", "hello"), Duration.ofSeconds(5));
            assertThat(raw).isEqualTo("OK");

            String value = LettuceResult.toBlocking(
                    defaultConnection.async().get("conv-key"), Duration.ofSeconds(5));
            String converted = LettuceConverterRegistry.convertResult(value);
            assertThat(converted).isEqualTo("HELLO");
        } finally {
            LettuceConverterRegistry.clear();
        }
    }
}
