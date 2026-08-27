package io.quarkus.redis.runtime.client.lettuce;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.netty.channel.EventLoopGroup;
import io.quarkus.redis.runtime.client.lettuce.datasource.LettuceBlockingRedisDataSourceImpl;
import io.quarkus.redis.runtime.client.lettuce.datasource.LettuceReactiveRedisDataSourceImpl;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Vertx;

public abstract class CommandsTestBase {

    protected static final Duration TIMEOUT = Duration.ofSeconds(5);
    protected static final String REDIS_DEFAULT_IMAGE = "redis:7-alpine";
    protected static final GenericContainer<?> REDIS = createContainer();

    protected static Vertx vertx;
    protected static LettuceClientResources lettuceResources;
    protected static RedisClient redisClient;
    protected static RedisURI redisUri;
    protected static RedisCodec<byte[], byte[]> codec;
    protected static StatefulRedisConnection<byte[], byte[]> connection;

    protected final String key = UUID.randomUUID().toString();

    @BeforeAll
    static void setUp() {
        if (REDIS.isRunning()) {
            return;
        }
        REDIS.start();

        vertx = Vertx.vertx();
        EventLoopGroup loops = ((VertxInternal) vertx.getDelegate()).eventLoopGroup();
        lettuceResources = new LettuceClientResources(loops);

        redisUri = RedisURI.create(REDIS.getHost(), REDIS.getFirstMappedPort());
        redisClient = RedisClient.create(lettuceResources.clientResources(), redisUri);
        codec = ByteArrayCodec.INSTANCE;
        connection = redisClient.connect(codec);
    }

    @AfterAll
    static void tearDown() {
        if (connection != null) {
            connection.close();
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
        if (REDIS.isRunning()) {
            REDIS.stop();
        }
    }

    @BeforeEach
    void flush() {
        connection.sync().flushall();
    }

    private static GenericContainer<?> createContainer() {
        GenericContainer<?> container = new GenericContainer<>(
                DockerImageName.parse(System.getProperty("redis.base.image", REDIS_DEFAULT_IMAGE)));
        container.withExposedPorts(6379);
        return container;
    }

    protected static CompletionStage<StatefulRedisConnection<byte[], byte[]>> connectAsync() {
        return redisClient.connectAsync(codec, redisUri);
    }

    protected static Supplier<CompletionStage<StatefulRedisConnection<byte[], byte[]>>> connector() {
        return CommandsTestBase::connectAsync;
    }

    protected static LettuceReactiveRedisDataSourceImpl reactiveDataSource() {
        return new LettuceReactiveRedisDataSourceImpl(vertx, connection, connector());
    }

    protected static LettuceBlockingRedisDataSourceImpl blockingDataSource() {
        return blockingDataSource(TIMEOUT);
    }

    protected static LettuceBlockingRedisDataSourceImpl blockingDataSource(Duration timeout) {
        return new LettuceBlockingRedisDataSourceImpl(reactiveDataSource(), timeout);
    }

    protected static long connectionCount() {
        String list = connection.sync().clientList();
        return list.isEmpty() ? 0 : list.split("\n").length;
    }

    protected static void rawSet(String key, String value) {
        connection.sync().set(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    protected static String rawGet(String key) {
        byte[] bytes = connection.sync().get(key.getBytes(StandardCharsets.UTF_8));
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

}
