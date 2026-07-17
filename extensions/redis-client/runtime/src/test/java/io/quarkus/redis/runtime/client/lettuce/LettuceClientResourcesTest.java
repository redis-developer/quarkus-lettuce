package io.quarkus.redis.runtime.client.lettuce;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.netty.channel.EventLoopGroup;
import io.smallrye.mutiny.Uni;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Vertx;

/**
 * Integration test for {@link LettuceClientResources} and {@link LettuceConnectionFactory}.
 * <p>
 * Validates:
 * <ul>
 * <li>Lettuce uses Vert.x-managed event loops (no Lettuce-owned I/O threads)</li>
 * <li>PING via Lettuce async API returns PONG</li>
 * <li>CompletionStage → Uni conversion works correctly</li>
 * <li>Shutdown ordering: client → resources → Vert.x</li>
 * </ul>
 */
@SuppressWarnings("resource")
class LettuceClientResourcesTest {

    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static Vertx vertx;
    static LettuceClientResources lettuceResources;
    static LettuceConnectionFactory connectionFactory;

    @BeforeAll
    static void setUp() {
        REDIS.start();
        vertx = Vertx.vertx();

        // Cast to VertxInternal to access eventLoopGroup() — Vert.x 5 removed the public
        // nettyEventLoopGroup() accessor; the internal API is the supported replacement.
        EventLoopGroup vertxEventLoops = ((VertxInternal) vertx.getDelegate()).eventLoopGroup();
        lettuceResources = new LettuceClientResources(vertxEventLoops);

        String redisUri = String.format("redis://%s:%d", REDIS.getHost(), REDIS.getFirstMappedPort());
        connectionFactory = new LettuceConnectionFactory(lettuceResources.clientResources(), redisUri);
    }

    @AfterAll
    static void tearDown() {
        // Shutdown ordering: client → resources → Vert.x
        if (connectionFactory != null) {
            connectionFactory.shutdown();
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
    void pingViaAsyncApi() throws Exception {
        try (StatefulRedisConnection<String, String> connection = connectionFactory.connect()) {
            RedisAsyncCommands<String, String> async = connection.async();

            CompletionStage<String> result = async.ping().toCompletableFuture();
            String pong = result.toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertThat(pong).isEqualTo("PONG");
        }
    }

    @Test
    void completionStageToUniConversion() {
        try (StatefulRedisConnection<String, String> connection = connectionFactory.connect()) {
            RedisAsyncCommands<String, String> async = connection.async();

            // Use Supplier form to preserve Uni laziness
            Uni<String> uni = Uni.createFrom().completionStage(() -> async.ping().toCompletableFuture());

            String pong = uni.await().atMost(Duration.ofSeconds(5));
            assertThat(pong).isEqualTo("PONG");
        }
    }

    @Test
    void setAndGetViaAsyncApi() {
        try (StatefulRedisConnection<String, String> connection = connectionFactory.connect()) {
            RedisAsyncCommands<String, String> async = connection.async();

            Uni<String> setResult = Uni.createFrom().completionStage(() -> async.set("test-key", "test-value"));
            assertThat(setResult.await().atMost(Duration.ofSeconds(5))).isEqualTo("OK");

            Uni<String> getResult = Uni.createFrom().completionStage(() -> async.get("test-key"));
            assertThat(getResult.await().atMost(Duration.ofSeconds(5))).isEqualTo("test-value");
        }
    }

    @Test
    void lettuceUsesVertxEventLoopThreads() throws Exception {
        try (StatefulRedisConnection<String, String> connection = connectionFactory.connect()) {
            RedisAsyncCommands<String, String> async = connection.async();

            // Execute a command and capture the thread name from the completion callback.
            // BLPOP on a missing key blocks server-side (~200ms), guaranteeing the future is
            // still pending when the callback is attached below. With an instant command like
            // PING, the response can arrive before thenAccept registers, and a callback added
            // to an already-completed future runs inline on the test thread — seen as a flaky
            // failure ("main" instead of an event-loop thread) on slower CI JVMs.
            CompletionStage<KeyValue<String, String>> future = async
                    .blpop(0.2, "lettuce-event-loop-test-missing-key").toCompletableFuture();
            String[] callbackThreadName = new String[1];

            future.thenAccept(result -> {
                callbackThreadName[0] = Thread.currentThread().getName();
            }).toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertThat(callbackThreadName[0])
                    .as("Lettuce callback should execute on a Vert.x event loop thread")
                    .contains("vert.x-eventloop");
        }
    }
}
