package io.quarkus.redis.runtime.client.lettuce.value;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.channel.EventLoopGroup;
import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.datasource.value.ValueCommands;
import io.quarkus.redis.runtime.client.lettuce.LettuceClientResources;
import io.quarkus.redis.runtime.client.lettuce.QuarkusRedisCodec;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Vertx;

/**
 * Integration test exercising {@link LettuceReactiveValueCommandsImpl} and
 * {@link LettuceBlockingValueCommandsImpl} against a real Redis instance.
 */
@SuppressWarnings("resource")
class LettuceValueCommandsIntegrationTest {

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static Vertx vertx;
    static LettuceClientResources lettuceResources;
    static RedisClient redisClient;
    static StatefulRedisConnection<String, String> connection;

    static LettuceReactiveValueCommandsImpl<String, String> reactive;
    static ValueCommands<String, String> blocking;

    @BeforeAll
    static void setUp() {
        REDIS.start();
        vertx = Vertx.vertx();
        EventLoopGroup loops = ((VertxInternal) vertx.getDelegate()).eventLoopGroup();
        lettuceResources = new LettuceClientResources(loops);

        String uri = String.format("redis://%s:%d", REDIS.getHost(), REDIS.getFirstMappedPort());
        redisClient = RedisClient.create(lettuceResources.clientResources(), uri);
        connection = redisClient.connect(new QuarkusRedisCodec<>(String.class, String.class));

        reactive = new LettuceReactiveValueCommandsImpl<>(null, connection);
        blocking = new LettuceBlockingValueCommandsImpl<>(null, reactive, TIMEOUT);
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
        REDIS.stop();
    }

    @BeforeEach
    void flush() {
        connection.sync().flushdb();
    }

    @Test
    void setAndGet() {
        blocking.set("k", "v");
        assertThat(blocking.get("k")).isEqualTo("v");
        assertThat(blocking.get("missing")).isNull();
    }

    @Test
    void setWithArgsNxXx() {
        assertThat(blocking.setAndChanged("k", "v1", new SetArgs().nx())).isTrue();
        assertThat(blocking.setAndChanged("k", "v2", new SetArgs().nx())).isFalse();
        assertThat(blocking.get("k")).isEqualTo("v1");

        assertThat(blocking.setAndChanged("k", "v3", new SetArgs().xx())).isTrue();
        assertThat(blocking.get("k")).isEqualTo("v3");

        assertThat(blocking.setAndChanged("absent", "v", new SetArgs().xx())).isFalse();
    }

    @Test
    void setWithEx() {
        blocking.set("k", "v", new SetArgs().ex(100));
        Long ttl = connection.sync().ttl("k");
        assertThat(ttl).isBetween(1L, 100L);
    }

    @Test
    void setGet() {
        assertThat(blocking.setGet("k", "first")).isNull();
        assertThat(blocking.setGet("k", "second")).isEqualTo("first");
        assertThat(blocking.get("k")).isEqualTo("second");
    }

    @Test
    void incrDecr() {
        assertThat(blocking.incr("counter")).isEqualTo(1L);
        assertThat(blocking.incrby("counter", 10)).isEqualTo(11L);
        assertThat(blocking.decr("counter")).isEqualTo(10L);
        assertThat(blocking.decrby("counter", 5)).isEqualTo(5L);
        assertThat(blocking.incrbyfloat("counter", 0.5)).isEqualTo(5.5);
    }

    @Test
    void appendAndStrlen() {
        assertThat(blocking.append("k", "hello")).isEqualTo(5L);
        assertThat(blocking.append("k", " world")).isEqualTo(11L);
        assertThat(blocking.strlen("k")).isEqualTo(11L);
    }

    @Test
    void mgetAndMset() {
        blocking.mset(Map.of("a", "1", "b", "2", "c", "3"));
        Map<String, String> result = blocking.mget("a", "b", "c", "missing");
        assertThat(result).containsEntry("a", "1").containsEntry("b", "2")
                .containsEntry("c", "3").containsEntry("missing", null);
    }

    @Test
    void msetnx() {
        assertThat(blocking.msetnx(Map.of("x", "1", "y", "2"))).isTrue();
        assertThat(blocking.msetnx(Map.of("y", "3", "z", "4"))).isFalse();
        assertThat(blocking.get("z")).isNull();
    }

    @Test
    void setexPsetexSetnx() {
        blocking.setex("ek", 100, "v");
        assertThat(connection.sync().ttl("ek")).isBetween(1L, 100L);

        blocking.psetex("pk", 100_000, "v");
        assertThat(connection.sync().pttl("pk")).isBetween(1L, 100_000L);

        assertThat(blocking.setnx("nx", "v1")).isTrue();
        assertThat(blocking.setnx("nx", "v2")).isFalse();
        assertThat(blocking.get("nx")).isEqualTo("v1");
    }

    @Test
    void getexPersistAndExpire() {
        blocking.set("k", "v", new SetArgs().ex(100));
        assertThat(blocking.getex("k", new GetExArgs().persist())).isEqualTo("v");
        assertThat(connection.sync().ttl("k")).isEqualTo(-1L);

        assertThat(blocking.getex("k", new GetExArgs().ex(50))).isEqualTo("v");
        assertThat(connection.sync().ttl("k")).isBetween(1L, 50L);
    }

    @Test
    void getdel() {
        blocking.set("k", "v");
        assertThat(blocking.getdel("k")).isEqualTo("v");
        assertThat(blocking.get("k")).isNull();
    }

    @Test
    void getrangeSetrange() {
        blocking.set("k", "Hello World");
        assertThat(blocking.getrange("k", 0, 4)).isEqualTo("Hello");
        assertThat(blocking.setrange("k", 6, "Redis")).isEqualTo(11L);
        assertThat(blocking.get("k")).isEqualTo("Hello Redis");
    }

    @Test
    @SuppressWarnings("deprecation")
    void getsetLegacy() {
        assertThat(blocking.getset("k", "first")).isNull();
        assertThat(blocking.getset("k", "second")).isEqualTo("first");
    }

    @Test
    void lcsNotYetImplemented() {
        assertThatThrownBy(() -> blocking.lcs("a", "b")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> blocking.lcsLength("a", "b")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void reactiveSurface() {
        // Smoke test that the reactive impl works directly (not just via the blocking wrapper)
        reactive.set("rk", "rv").await().atMost(TIMEOUT);
        assertThat(reactive.get("rk").await().atMost(TIMEOUT)).isEqualTo("rv");
        assertThat(reactive.strlen("rk").await().atMost(TIMEOUT)).isEqualTo(2L);
    }

    @Test
    void dataSourceAccessors() {
        // Constructor arg was null in this test — accessor should return it
        assertThat(reactive.getDataSource()).isNull();
        assertThat(blocking.getDataSource()).isNull();
    }
}
