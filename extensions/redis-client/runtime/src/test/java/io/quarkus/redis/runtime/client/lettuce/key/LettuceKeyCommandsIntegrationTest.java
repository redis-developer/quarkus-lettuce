package io.quarkus.redis.runtime.client.lettuce.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.channel.EventLoopGroup;
import io.quarkus.redis.datasource.keys.CopyArgs;
import io.quarkus.redis.datasource.keys.ExpireArgs;
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.datasource.keys.KeyScanCursor;
import io.quarkus.redis.datasource.keys.RedisKeyNotFoundException;
import io.quarkus.redis.datasource.keys.RedisValueType;
import io.quarkus.redis.runtime.client.lettuce.LettuceClientResources;
import io.quarkus.redis.runtime.client.lettuce.QuarkusRedisCodec;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;

/**
 * Integration test exercising {@link LettuceReactiveKeyCommandsImpl} and
 * {@link LettuceBlockingKeyCommandsImpl} against a real Redis instance.
 */
@SuppressWarnings("resource")
class LettuceKeyCommandsIntegrationTest {

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static Vertx vertx;
    static LettuceClientResources lettuceResources;
    static RedisClient redisClient;
    static StatefulRedisConnection<String, String> connection;

    static LettuceReactiveKeyCommandsImpl<String, String> reactive;
    static KeyCommands<String> blocking;

    @BeforeAll
    static void setUp() {
        REDIS.start();
        vertx = Vertx.vertx();
        EventLoopGroup loops = ((VertxInternal) vertx.getDelegate()).getEventLoopGroup();
        lettuceResources = new LettuceClientResources(loops);

        String uri = String.format("redis://%s:%d", REDIS.getHost(), REDIS.getFirstMappedPort());
        redisClient = RedisClient.create(lettuceResources.clientResources(), uri);
        connection = redisClient.connect(new QuarkusRedisCodec<>(String.class, String.class));

        reactive = new LettuceReactiveKeyCommandsImpl<>(null, connection);
        blocking = new LettuceBlockingKeyCommandsImpl<>(null, reactive, TIMEOUT);
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
    void existsAndDel() {
        connection.sync().set("a", "1");
        connection.sync().set("b", "2");
        assertThat(blocking.exists("a")).isTrue();
        assertThat(blocking.exists("missing")).isFalse();
        assertThat(blocking.exists("a", "b", "missing")).isEqualTo(2);
        assertThat(blocking.del("a", "b", "missing")).isEqualTo(2);
        assertThat(blocking.exists("a")).isFalse();
    }

    @Test
    void unlinkAndTouch() {
        connection.sync().mset(java.util.Map.of("a", "1", "b", "2", "c", "3"));
        assertThat(blocking.touch("a", "b", "missing")).isEqualTo(2);
        assertThat(blocking.unlink("a", "b", "missing")).isEqualTo(2);
        assertThat(blocking.exists("c")).isTrue();
    }

    @Test
    void expireAndTtl() {
        connection.sync().set("k", "v");
        assertThat(blocking.expire("k", 100)).isTrue();
        assertThat(blocking.ttl("k")).isBetween(1L, 100L);

        assertThat(blocking.expire("k", Duration.ofSeconds(50))).isTrue();
        assertThat(blocking.ttl("k")).isBetween(1L, 50L);

        assertThat(blocking.expire("missing", 100)).isFalse();
    }

    @Test
    void expireWithArgs() {
        connection.sync().set("k", "v");
        // NX: only set when no expiry
        assertThat(blocking.expire("k", 100, new ExpireArgs().nx())).isTrue();
        assertThat(blocking.expire("k", 200, new ExpireArgs().nx())).isFalse();
        // XX: only when expiry exists
        assertThat(blocking.expire("k", 300, new ExpireArgs().xx())).isTrue();
        // GT: only if greater
        assertThat(blocking.expire("k", 100, new ExpireArgs().gt())).isFalse();
        assertThat(blocking.expire("k", 500, new ExpireArgs().gt())).isTrue();
        // LT: only if less
        assertThat(blocking.expire("k", 1000, new ExpireArgs().lt())).isFalse();
        assertThat(blocking.expire("k", 100, new ExpireArgs().lt())).isTrue();
        // Duration + ExpireArgs overload
        assertThat(blocking.expire("k", Duration.ofSeconds(50), new ExpireArgs().lt())).isTrue();
    }

    @Test
    void expireatAndExpiretime() {
        connection.sync().set("k", "v");
        long target = Instant.now().plusSeconds(100).getEpochSecond();
        assertThat(blocking.expireat("k", target)).isTrue();
        assertThat(blocking.expiretime("k")).isEqualTo(target);

        Instant later = Instant.now().plusSeconds(200);
        assertThat(blocking.expireat("k", later)).isTrue();
        assertThat(blocking.expiretime("k")).isEqualTo(later.getEpochSecond());

        long greater = Instant.now().plusSeconds(500).getEpochSecond();
        assertThat(blocking.expireat("k", greater, new ExpireArgs().gt())).isTrue();
        assertThat(blocking.expiretime("k")).isEqualTo(greater);

        Instant lower = Instant.now().plusSeconds(100);
        assertThat(blocking.expireat("k", lower, new ExpireArgs().lt())).isTrue();
        assertThat(blocking.expiretime("k")).isEqualTo(lower.getEpochSecond());
    }

    @Test
    void pexpireAndPttl() {
        connection.sync().set("k", "v");
        assertThat(blocking.pexpire("k", 100_000)).isTrue();
        assertThat(blocking.pttl("k")).isBetween(1L, 100_000L);

        assertThat(blocking.pexpire("k", Duration.ofMillis(50_000))).isTrue();
        assertThat(blocking.pttl("k")).isBetween(1L, 50_000L);

        assertThat(blocking.pexpire("k", 200_000L, new ExpireArgs().gt())).isTrue();
        assertThat(blocking.pttl("k")).isBetween(1L, 200_000L);

        assertThat(blocking.pexpire("k", Duration.ofMillis(10_000), new ExpireArgs().lt())).isTrue();
        assertThat(blocking.pttl("k")).isBetween(1L, 10_000L);
    }

    @Test
    void pexpireatAndPexpiretime() {
        connection.sync().set("k", "v");
        long target = Instant.now().plusSeconds(100).toEpochMilli();
        assertThat(blocking.pexpireat("k", target)).isTrue();
        assertThat(blocking.pexpiretime("k")).isEqualTo(target);

        Instant later = Instant.now().plusSeconds(200);
        assertThat(blocking.pexpireat("k", later)).isTrue();
        assertThat(blocking.pexpiretime("k")).isEqualTo(later.toEpochMilli());

        long greater = Instant.now().plusSeconds(500).toEpochMilli();
        assertThat(blocking.pexpireat("k", greater, new ExpireArgs().gt())).isTrue();
        assertThat(blocking.pexpiretime("k")).isEqualTo(greater);

        Instant lower = Instant.now().plusSeconds(100);
        assertThat(blocking.pexpireat("k", lower, new ExpireArgs().lt())).isTrue();
        assertThat(blocking.pexpiretime("k")).isEqualTo(lower.toEpochMilli());
    }

    @Test
    void moveBetweenDatabases() {
        connection.sync().select(0);
        connection.sync().set("k", "v");
        assertThat(blocking.move("k", 1)).isTrue();
        assertThat(connection.sync().exists("k")).isEqualTo(0L);
        connection.sync().select(1);
        try {
            assertThat(connection.sync().get("k")).isEqualTo("v");
        } finally {
            connection.sync().del("k");
            connection.sync().select(0);
        }
        assertThat(blocking.move("missing", 1)).isFalse();
    }

    @Test
    void persist() {
        connection.sync().set("k", "v");
        connection.sync().expire("k", 100L);
        assertThat(blocking.persist("k")).isTrue();
        assertThat(connection.sync().ttl("k")).isEqualTo(-1L);
        assertThat(blocking.persist("k")).isFalse();
    }

    @Test
    void ttlThrowsForMissingKey() {
        assertThatThrownBy(() -> blocking.ttl("missing")).isInstanceOf(RedisKeyNotFoundException.class);
        assertThatThrownBy(() -> blocking.pttl("missing")).isInstanceOf(RedisKeyNotFoundException.class);
        assertThatThrownBy(() -> blocking.expiretime("missing")).isInstanceOf(RedisKeyNotFoundException.class);
        assertThatThrownBy(() -> blocking.pexpiretime("missing")).isInstanceOf(RedisKeyNotFoundException.class);
    }

    @Test
    void renameAndRenamenx() {
        connection.sync().set("src", "v");
        blocking.rename("src", "dst");
        assertThat(blocking.exists("src")).isFalse();
        assertThat(connection.sync().get("dst")).isEqualTo("v");

        connection.sync().set("a", "1");
        connection.sync().set("b", "2");
        assertThat(blocking.renamenx("a", "b")).isFalse();
        assertThat(blocking.renamenx("a", "c")).isTrue();
    }

    @Test
    void renameThrowsForMissingKey() {
        assertThatThrownBy(() -> blocking.rename("missing", "dst")).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void copyAndCopyWithArgs() {
        connection.sync().set("src", "v");
        assertThat(blocking.copy("src", "dst")).isTrue();
        assertThat(connection.sync().get("dst")).isEqualTo("v");

        // REPLACE
        connection.sync().set("dst", "existing");
        assertThat(blocking.copy("src", "dst")).isFalse();
        assertThat(blocking.copy("src", "dst", new CopyArgs().replace(true))).isTrue();
        assertThat(connection.sync().get("dst")).isEqualTo("v");
    }

    @Test
    void typeAndRandomkey() {
        connection.sync().set("s", "v");
        connection.sync().lpush("l", "a");
        connection.sync().sadd("st", "a");
        assertThat(blocking.type("s")).isEqualTo(RedisValueType.STRING);
        assertThat(blocking.type("l")).isEqualTo(RedisValueType.LIST);
        assertThat(blocking.type("st")).isEqualTo(RedisValueType.SET);
        assertThat(blocking.type("missing")).isEqualTo(RedisValueType.NONE);

        assertThat(blocking.randomkey()).isNotNull();
    }

    @Test
    void keys() {
        connection.sync().mset(java.util.Map.of("user:1", "a", "user:2", "b", "other", "c"));
        assertThat(blocking.keys("user:*")).containsExactlyInAnyOrder("user:1", "user:2");
    }

    @Test
    void scanIterates() {
        for (int i = 0; i < 20; i++) {
            connection.sync().set("k:" + i, "v");
        }
        KeyScanCursor<String> cursor = blocking.scan(new KeyScanArgs().match("k:*").count(5));
        java.util.Set<String> collected = new java.util.HashSet<>();
        while (cursor.hasNext()) {
            collected.addAll(cursor.next());
        }
        assertThat(collected).hasSize(20);
    }

    @Test
    void dumpReturnsSerialisedPayload() {
        connection.sync().set("k", "v");
        String dump = blocking.dump("k");
        assertThat(dump).isNotNull().isNotEmpty();
        assertThat(blocking.dump("missing")).isNull();
    }

    @Test
    void reactiveSurface() {
        connection.sync().set("rk", "rv");
        assertThat(reactive.expire("rk", 100L).await().atMost(TIMEOUT)).isTrue();
        assertThat(reactive.ttl("rk").await().atMost(TIMEOUT)).isBetween(1L, 100L);
        assertThat(reactive.del("rk").await().atMost(TIMEOUT)).isEqualTo(1);
    }

    @Test
    void dataSourceAccessors() {
        assertThat(reactive.getDataSource()).isNull();
        assertThat(blocking.getDataSource()).isNull();
    }
}
