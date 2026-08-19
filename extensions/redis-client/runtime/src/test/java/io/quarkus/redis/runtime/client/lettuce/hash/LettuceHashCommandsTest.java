package io.quarkus.redis.runtime.client.lettuce.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.offset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.hash.HashCommands;
import io.quarkus.redis.datasource.hash.HashScanCursor;
import io.quarkus.redis.datasource.hash.ReactiveHashCommands;
import io.quarkus.redis.datasource.hash.ReactiveHashScanCursor;
import io.quarkus.redis.runtime.client.lettuce.CommandsTestBase;

class LettuceHashCommandsTest extends CommandsTestBase {

    private static final String HELLO = "hello";
    private static final String WORLD = "world";
    private static final String OTHER = "other";

    private final String key = UUID.randomUUID().toString();

    private RedisDataSource blockingDs;
    private ReactiveRedisDataSource reactiveDs;
    private HashCommands<String, String, String> blocking;
    private ReactiveHashCommands<String, String, String> reactive;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        blockingDs = blockingDataSource();
        reactive = reactiveDs.hash(String.class);
        blocking = blockingDs.hash(String.class);
    }

    @Test
    void getDataSource() {
        assertThat(blockingDs).isEqualTo(blocking.getDataSource());
        assertThat(reactiveDs).isEqualTo(reactive.getDataSource());
    }

    @Test
    void simpleHset() {
        blocking.hset("my-hash", "field1", HELLO);
        String value = blocking.hget("my-hash", "field1");
        assertThat(value).isEqualTo(HELLO);

        assertThat(blocking.hdel("my-hash", "field1")).isEqualTo(1);
        value = blocking.hget("my-hash", "field1");
        assertThat(value).isNull();
    }

    @Test
    void hsetWithTypeReference() {
        var h = blockingDs.hash(new TypeReference<String>() {
        });
        h.hset("my-hash", "l1", HELLO);
        assertThat(h.hget("my-hash", "l1")).isEqualTo(HELLO);

        assertThat(h.hdel("my-hash", "l1")).isEqualTo(1);
        assertThat(h.hget("my-hash", "l1")).isNull();
    }

    @Test
    void hdel() {
        assertThat(blocking.hdel(key, "one")).isEqualTo(0);
        blocking.hset(key, "two", HELLO);
        assertThat(blocking.hdel(key, "one")).isEqualTo(0);
        blocking.hset(key, "one", WORLD);
        assertThat(blocking.hdel(key, "one")).isEqualTo(1);
        blocking.hset(key, "one", WORLD);
        assertThat(blocking.hdel(key, "one", "two")).isEqualTo(2);
    }

    @Test
    void hexists() {
        assertThat(blocking.hexists(key, "one")).isFalse();
        blocking.hset(key, "two", WORLD);
        assertThat(blocking.hexists(key, "one")).isFalse();
        blocking.hset(key, "one", HELLO);
        assertThat(blocking.hexists(key, "one")).isTrue();
    }

    @Test
    void hget() {
        assertThat(blocking.hget(key, "one")).isNull();
        blocking.hset(key, "one", HELLO);
        assertThat(blocking.hget(key, "one")).isEqualTo(HELLO);
    }

    @Test
    void hgetall() {
        assertThat(blocking.hgetall(key).isEmpty()).isTrue();

        blocking.hset(key, "zero", OTHER);
        blocking.hset(key, "one", HELLO);
        blocking.hset(key, "two", WORLD);

        Map<String, String> map = blocking.hgetall(key);

        assertThat(map).hasSize(3);
        assertThat(map.keySet()).containsExactlyInAnyOrder("zero", "one", "two");
        assertThat(map.values()).containsExactlyInAnyOrder(OTHER, HELLO, WORLD);

        assertThat(blocking.hgetall("missing")).isEmpty();
    }

    @Test
    void hincrby() {
        assertThat(blocking.hincrby(key, "one", 1)).isEqualTo(1);
        assertThat(blocking.hincrby(key, "one", -2)).isEqualTo(-1);
    }

    @Test
    void hincrbyfloat() {
        assertThat(blocking.hincrbyfloat(key, "one", 1.0)).isEqualTo(1.0);
        assertThat(blocking.hincrbyfloat(key, "one", -2.0)).isEqualTo(-1.0);
        assertThat(blocking.hincrbyfloat(key, "one", 1.23)).isEqualTo(0.23, offset(0.001));
    }

    @Test
    void hkeys() {
        populate();
        List<String> keys = blocking.hkeys(key);
        assertThat(keys).hasSize(2);
        assertThat(keys).containsExactly("one", "two");
    }

    private void populate() {
        assertThat(blocking.hkeys(key)).isEqualTo(Collections.emptyList());
        blocking.hset(key, "one", HELLO);
        blocking.hset(key, "two", WORLD);
    }

    @Test
    void hlen() {
        assertThat(blocking.hlen(key)).isEqualTo(0);
        blocking.hset(key, "one", HELLO);
        assertThat(blocking.hlen(key)).isEqualTo(1);
    }

    @Test
    void hstrlen() {
        assertThat(blocking.hstrlen(key, "one")).isEqualTo(0);
        blocking.hset(key, "one", HELLO);
        assertThat(blocking.hstrlen(key, "one")).isEqualTo(HELLO.length());
    }

    @Test
    void hmget() {
        populateForHmget();
        Map<String, String> values = blocking.hmget(key, "one", "missing", "two");
        assertThat(values).hasSize(3);
        assertThat(values).containsExactly(entry("one", HELLO), entry("missing", null), entry("two", WORLD));
    }

    private void populateForHmget() {
        assertThat(blocking.hmget(key, "one", "two")).allSatisfy((f, v) -> assertThat(v).isNull());
        blocking.hset(key, "one", HELLO);
        blocking.hset(key, "two", WORLD);
    }

    @Test
    void hmset() {
        blocking.hmset(key, Map.of("one", HELLO, "two", WORLD));
        assertThat(blocking.hmget(key, "one", "two")).containsExactly(entry("one", HELLO), entry("two", WORLD));
    }

    @Test
    void hmsetWithNulls() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("one", null);
        blocking.hmset(key, map);
        assertThat(blocking.hmget(key, "one")).containsExactly(entry("one", ""));

        map.put("one", HELLO);
        blocking.hmset(key, map);
        assertThat(blocking.hmget(key, "one")).containsExactly(entry("one", HELLO));
    }

    @Test
    void hrandfield() {
        blocking.hset(key, Map.of("one", HELLO, "two", WORLD, "three", OTHER));

        assertThat(blocking.hrandfield(key)).isIn("one", "two", "three");
        assertThat(blocking.hrandfield(key, 2)).hasSize(2).containsAnyOf("one", "two", "three");
    }

    @Test
    void hrandfieldWithValues() {
        Map<String, String> map = Map.of("one", HELLO, "two", WORLD, "three", OTHER);
        blocking.hset(key, map);

        assertThat(blocking.hrandfieldWithValues(key, 1))
                .anySatisfy((f, v) -> assertThat(map.get(f)).isEqualTo(v));
        assertThat(blocking.hrandfieldWithValues(key, 2)).hasSize(2)
                .allSatisfy((f, v) -> assertThat(map.get(f)).isEqualTo(v));

        assertThat(blocking.hrandfieldWithValues(key, -20)).isNotEmpty();
        assertThat(blocking.hrandfieldWithValues(key, 3)).containsExactlyInAnyOrderEntriesOf(map);

        assertThat(blocking.hrandfieldWithValues("missing", 3)).isEmpty();
    }

    @Test
    void hset() {
        assertThat(blocking.hset(key, "one", HELLO)).isTrue();
        assertThat(blocking.hset(key, "one", HELLO)).isFalse();
    }

    @Test
    void hsetMap() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("two", WORLD);
        map.put("three", OTHER);
        assertThat(blocking.hset(key, map)).isEqualTo(2);

        map.put("two", WORLD);
        assertThat(blocking.hset(key, map)).isEqualTo(0);
        assertThat(blocking.hget(key, "two")).isEqualTo(WORLD);
    }

    @Test
    void hsetnx() {
        blocking.hset(key, "one", HELLO);
        assertThat(blocking.hsetnx(key, "one", WORLD)).isFalse();
        assertThat(blocking.hget(key, "one")).isEqualTo(HELLO);
    }

    @Test
    void hvals() {
        assertThat(blocking.hvals(key)).isEqualTo(List.of());
        blocking.hset(key, "one", HELLO);
        blocking.hset(key, "two", WORLD);
        List<String> values = blocking.hvals(key);
        assertThat(values).hasSize(2).containsExactly(HELLO, WORLD);
    }

    @Test
    void hscan() {
        blocking.hset(key, "one", OTHER);
        HashScanCursor<String, String> cursor = blocking.hscan(key);

        assertThat(cursor.hasNext()).isTrue();
        Map<String, String> next = cursor.next();

        assertThat(next).containsExactly(entry("one", OTHER));
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void hscanEmpty() {
        HashScanCursor<String, String> cursor = blocking.hscan(key);

        assertThat(cursor.hasNext()).isTrue();
        Map<String, String> next = cursor.next();

        assertThat(cursor.hasNext()).isFalse();
        assertThat(next).isEmpty();
    }

    @Test
    void hscanAsIteratorEmpty() {
        HashScanCursor<String, String> cursor = blocking.hscan(key);
        Iterable<Map.Entry<String, String>> iterable = cursor.toIterable();

        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, String> entry : iterable) {
            keys.add(entry.getKey());
        }
        assertThat(keys).isEmpty();
    }

    @Test
    void hscanWithArgs() {
        blocking.hset(key, "one", OTHER);
        blocking.hset(key, "two", HELLO);
        blocking.hset(key, "three", WORLD);
        HashScanCursor<String, String> cursor = blocking.hscan(key, new ScanArgs().count(3));

        assertThat(cursor.hasNext()).isTrue();
        Map<String, String> next = cursor.next();

        assertThat(next).containsExactly(entry("one", OTHER), entry("two", HELLO), entry("three", WORLD));
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void hscanMultiple() {
        Map<String, String> expect = new LinkedHashMap<>();
        Map<String, String> check = new LinkedHashMap<>();
        populateManyEntries(expect);

        HashScanCursor<String, String> cursor = blocking.hscan(key, new ScanArgs().count(5));
        while (cursor.hasNext()) {
            check.putAll(cursor.next());
        }

        assertThat(check).isEqualTo(expect);
    }

    @Test
    void hscanIterator() {
        Map<String, String> expect = new LinkedHashMap<>();
        Map<String, String> check = new LinkedHashMap<>();
        populateManyEntries(expect);

        HashScanCursor<String, String> cursor = blocking.hscan(key, new ScanArgs().count(5));
        Iterable<Map.Entry<String, String>> entries = cursor.toIterable();
        for (Map.Entry<String, String> entry : entries) {
            check.put(entry.getKey(), entry.getValue());
        }

        assertThat(cursor.hasNext()).isFalse();
        assertThat(check).isEqualTo(expect);
    }

    @Test
    void hscanMatch() {
        Map<String, String> expect = new LinkedHashMap<>();
        Map<String, String> check = new HashMap<>();

        populateManyEntries(expect);

        HashScanCursor<String, String> cursor = blocking.hscan(key, new ScanArgs().match("f1*"));
        while (cursor.hasNext()) {
            check.putAll(cursor.next());
        }

        assertThat(check).hasSize(11);
    }

    private void populateManyEntries(Map<String, String> expect) {
        for (int i = 0; i < 100; i++) {
            expect.put("f" + i, "hello" + i);
        }
        blocking.hset(key, expect);
    }

    /**
     * Reproducer for <a href="https://github.com/quarkusio/quarkus/issues/42131">#42131</a>.
     */
    @Test
    void testInvalidHashMGet() {
        HashCommands<String, String, String> cmd = blockingDs.hash(String.class, String.class, String.class);
        // Key must not be null
        assertThatThrownBy(() -> cmd.hmget(null, "a", "b")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key");
        // Fields must not be empty
        assertThatThrownBy(() -> cmd.hmget("key")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fields");

        // Fields must not contain `null`
        assertThatThrownBy(() -> cmd.hmget("key", null, "b")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fields");
        assertThatThrownBy(() -> cmd.hmget("key", "a", null, "b")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fields");
    }

    @Test
    void fieldTypeMustMatchKeyType() {
        assertThatThrownBy(() -> blockingDs.hash(String.class, Integer.class, String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field type");
        assertThatThrownBy(() -> reactiveDs.hash(String.class, Integer.class, String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field type");
    }

    @Test
    void hscanReactiveToMulti() {
        Map<String, String> expect = new LinkedHashMap<>();
        populateManyEntries(expect);

        ReactiveHashScanCursor<String, String> cursor = reactive.hscan(key, new ScanArgs().count(5));
        List<Map.Entry<String, String>> entries = cursor.toMulti().collect().asList().await().atMost(TIMEOUT);

        Map<String, String> check = new LinkedHashMap<>();
        entries.forEach(e -> check.put(e.getKey(), e.getValue()));
        assertThat(check).isEqualTo(expect);
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void hscanCursorId() {
        blocking.hset(key, "one", HELLO);

        ReactiveHashScanCursor<String, String> cursor = reactive.hscan(key);
        assertThat(cursor.cursorId()).isEqualTo(0L);

        cursor.next().await().atMost(TIMEOUT);

        assertThat(cursor.hasNext()).isFalse();
        assertThat(cursor.cursorId()).isEqualTo(0L);
    }

}
