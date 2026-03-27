package io.quarkus.redis.runtime.client.lettuce;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.netty.channel.EventLoopGroup;
import io.smallrye.mutiny.Uni;
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

        EventLoopGroup vertxEventLoops = vertx.getDelegate().nettyEventLoopGroup();
        lettuceResources = new LettuceClientResources(vertxEventLoops);

        String redisUri = String.format("redis://%s:%d", REDIS.getHost(), REDIS.getFirstMappedPort());
        connectionFactory = new LettuceConnectionFactory(lettuceResources.get(), redisUri);
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
            String pong = result.toCompletableFuture().get();

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

            // Execute a command and capture the thread name from the completion callback
            CompletionStage<String> future = async.ping().toCompletableFuture();
            String[] callbackThreadName = new String[1];

            future.thenAccept(result -> {
                callbackThreadName[0] = Thread.currentThread().getName();
            }).toCompletableFuture().get();

            assertThat(callbackThreadName[0])
                    .as("Lettuce callback should execute on a Vert.x event loop thread")
                    .contains("vert.x-eventloop");
        }
    }
}
