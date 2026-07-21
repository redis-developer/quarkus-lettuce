package io.quarkus.redis.runtime.client.lettuce.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
import io.quarkus.redis.datasource.Person;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.hash.HashCommands;
import io.quarkus.redis.datasource.hash.ReactiveHashScanCursor;
import io.quarkus.redis.runtime.client.lettuce.LettuceClientResources;
import io.quarkus.redis.runtime.client.lettuce.QuarkusRedisCodec;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Vertx;

/**
 * Integration test exercising {@link LettuceReactiveHashCommandsImpl} / {@link LettuceBlockingHashCommandsImpl}
 * (and {@link LettuceHashScanCursor}) against a real Redis instance.
 * <p>
 * Uses {@code <String, String, Person>} so the value codec marshals a POJO, stressing the {@code F}/{@code K}
 * field/key bridge and the codec round-trip that the compiler cannot verify.
 */
@SuppressWarnings("resource")
class LettuceHashCommandsIntegrationTest {

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static Vertx vertx;
    static LettuceClientResources lettuceResources;
    static RedisClient redisClient;
    static StatefulRedisConnection<String, Person> connection;

    static LettuceReactiveHashCommandsImpl<String, String, Person> reactive;
    static HashCommands<String, String, Person> blocking;

    @BeforeAll
    static void setUp() {
        REDIS.start();
        vertx = Vertx.vertx();
        EventLoopGroup loops = ((VertxInternal) vertx.getDelegate()).eventLoopGroup();
        lettuceResources = new LettuceClientResources(loops);

        String uri = String.format("redis://%s:%d", REDIS.getHost(), REDIS.getFirstMappedPort());
        redisClient = RedisClient.create(lettuceResources.clientResources(), uri);
        connection = redisClient.connect(new QuarkusRedisCodec<>(String.class, Person.class));

        reactive = new LettuceReactiveHashCommandsImpl<>(null, connection);
        blocking = new LettuceBlockingHashCommandsImpl<>(null, reactive, TIMEOUT);
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

    // Round-trips a field (F) and a POJO value (V): validates the F/K codec bridge end-to-end.
    @Test
    void hsetHgetHexists() {
        assertThat(blocking.hset("h", "field1", Person.person1)).isTrue();
        assertThat(blocking.hget("h", "field1")).isEqualTo(Person.person1);
        assertThat(blocking.hget("h", "missing")).isNull();
        assertThat(blocking.hexists("h", "field1")).isTrue();
        assertThat(blocking.hexists("h", "missing")).isFalse();
    }

    @Test
    void hgetall() {
        blocking.hset("h", "one", Person.person1);
        blocking.hset("h", "two", Person.person2);
        assertThat(blocking.hgetall("h"))
                .containsOnly(entry("one", Person.person1), entry("two", Person.person2));
    }

    // Lettuce hdel returns Long; the interface returns int (the .map(Long::intValue) adapter).
    @Test
    void hdelReturnsCount() {
        blocking.hset("h", "one", Person.person1);
        blocking.hset("h", "two", Person.person2);
        assertThat(blocking.hdel("h", "one", "two", "missing")).isEqualTo(2);
        assertThat(blocking.hexists("h", "one")).isFalse();
    }

    // Lettuce hmset returns "OK" (String); the interface returns void (the .replaceWithVoid() adapter).
    @Test
    void hmsetThenHgetall() {
        blocking.hmset("h", Map.of("one", Person.person1, "two", Person.person2, "three", Person.person3));
        assertThat(blocking.hgetall("h")).hasSize(3)
                .containsEntry("one", Person.person1)
                .containsEntry("two", Person.person2)
                .containsEntry("three", Person.person3);
    }

    // Lettuce returns List<KeyValue>; interface returns Map. A missing field maps to null, order preserved.
    @Test
    void hmgetIncludingMissingField() {
        blocking.hset("h", "one", Person.person1);
        blocking.hset("h", "two", Person.person2);
        Map<String, Person> result = blocking.hmget("h", "one", "missing", "two");
        assertThat(result).hasSize(3)
                .containsExactly(entry("one", Person.person1), entry("missing", null), entry("two", Person.person2));
    }

    @Test
    void hkeysHvalsHlen() {
        blocking.hset("h", "one", Person.person1);
        blocking.hset("h", "two", Person.person2);
        assertThat(blocking.hlen("h")).isEqualTo(2);
        assertThat(blocking.hkeys("h")).containsExactlyInAnyOrder("one", "two");
        assertThat(blocking.hvals("h")).containsExactlyInAnyOrder(Person.person1, Person.person2);
    }

    @Test
    void hsetnx() {
        assertThat(blocking.hsetnx("h", "one", Person.person1)).isTrue();
        assertThat(blocking.hsetnx("h", "one", Person.person2)).isFalse();
        assertThat(blocking.hget("h", "one")).isEqualTo(Person.person1);
    }

    @Test
    void hrandfield() {
        blocking.hset("h", "one", Person.person1);
        blocking.hset("h", "two", Person.person2);
        assertThat(blocking.hrandfield("h")).isIn("one", "two");
        assertThat(blocking.hrandfield("h", 2)).containsExactlyInAnyOrder("one", "two");
    }

    // Lettuce returns List<KeyValue>; interface returns Map (the toMap adapter).
    @Test
    void hrandfieldWithValues() {
        blocking.hset("h", "one", Person.person1);
        blocking.hset("h", "two", Person.person2);
        assertThat(blocking.hrandfieldWithValues("h", 2))
                .containsOnly(entry("one", Person.person1), entry("two", Person.person2));
    }

    // The hand-written cursor: iterate a large hash to completion and confirm every field is returned once.
    @Test
    void hscanIteratesAllFields() {
        Map<String, Person> data = new LinkedHashMap<>();
        for (int i = 0; i < 200; i++) {
            data.put("f" + i, new Person("first" + i, "last" + i));
        }
        reactive.hmset("h", data).await().atMost(TIMEOUT);

        ReactiveHashScanCursor<String, Person> cursor = reactive.hscan("h");
        Map<String, Person> collected = new HashMap<>();
        assertThat(cursor.hasNext()).isTrue();
        while (cursor.hasNext()) {
            collected.putAll(cursor.next().await().atMost(TIMEOUT));
        }
        assertThat(cursor.hasNext()).isFalse();
        assertThat(collected).isEqualTo(data);
    }

    // Empty hash: one emission of an empty batch, then done (the `initial` flag).
    @Test
    void hscanOnEmptyHash() {
        ReactiveHashScanCursor<String, Person> cursor = reactive.hscan("missing-hash");
        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next().await().atMost(TIMEOUT)).isEmpty();
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void hscanWithMatch() {
        blocking.hset("h", "keep:1", Person.person1);
        blocking.hset("h", "keep:2", Person.person2);
        blocking.hset("h", "skip:1", Person.person3);

        ReactiveHashScanCursor<String, Person> cursor = reactive.hscan("h", new ScanArgs().match("keep:*"));
        Map<String, Person> collected = new HashMap<>();
        while (cursor.hasNext()) {
            collected.putAll(cursor.next().await().atMost(TIMEOUT));
        }
        assertThat(collected).containsOnlyKeys("keep:1", "keep:2");
    }

    @Test
    void hscanToMulti() {
        Map<String, Person> data = new LinkedHashMap<>();
        for (int i = 0; i < 200; i++) {
            data.put("f" + i, new Person("first" + i, "last" + i));
        }
        reactive.hmset("h", data).await().atMost(TIMEOUT);

        List<Map.Entry<String, Person>> entries = reactive.hscan("h").toMulti()
                .collect().asList().await().atMost(TIMEOUT);
        assertThat(entries).hasSize(200);
    }
}
