package io.quarkus.redis.runtime.client.lettuce;

import java.time.Duration;
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
    protected static RedisCodec<String, String> codec;
    protected static StatefulRedisConnection<String, String> connection;

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
        codec = new QuarkusRedisCodec<>(String.class, String.class);
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

    protected static CompletionStage<StatefulRedisConnection<String, String>> connectAsync() {
        return redisClient.connectAsync(codec, redisUri);
    }

    protected static Supplier<CompletionStage<StatefulRedisConnection<String, String>>> connector() {
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

}
