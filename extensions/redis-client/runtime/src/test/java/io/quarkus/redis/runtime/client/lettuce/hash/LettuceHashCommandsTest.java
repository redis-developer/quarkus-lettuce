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

    static final String HELLO = "hello";
    static final String WORLD = "world";
    static final String OTHER = "other";

    ReactiveRedisDataSource reactiveDs;
    RedisDataSource blockingDs;
    ReactiveHashCommands<String, String, String> reactiveHash;
    HashCommands<String, String, String> blockingHash;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        blockingDs = blockingDataSource();
        reactiveHash = reactiveDs.hash(String.class);
        blockingHash = blockingDs.hash(String.class);
    }

    @Test
    void getDataSource() {
        assertThat(reactiveDs).isEqualTo(reactiveHash.getDataSource());
        assertThat(blockingDs).isEqualTo(blockingHash.getDataSource());
    }

    @Test
    void simpleHset() {
        blockingHash.hset("my-hash", "field1", HELLO);
        String value = blockingHash.hget("my-hash", "field1");
        assertThat(value).isEqualTo(HELLO);

        assertThat(blockingHash.hdel("my-hash", "field1")).isEqualTo(1);
        value = blockingHash.hget("my-hash", "field1");
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
        assertThat(blockingHash.hdel(key, "one")).isEqualTo(0);
        blockingHash.hset(key, "two", HELLO);
        assertThat(blockingHash.hdel(key, "one")).isEqualTo(0);
        blockingHash.hset(key, "one", WORLD);
        assertThat(blockingHash.hdel(key, "one")).isEqualTo(1);
        blockingHash.hset(key, "one", WORLD);
        assertThat(blockingHash.hdel(key, "one", "two")).isEqualTo(2);
    }

    @Test
    void hexists() {
        assertThat(blockingHash.hexists(key, "one")).isFalse();
        blockingHash.hset(key, "two", WORLD);
        assertThat(blockingHash.hexists(key, "one")).isFalse();
        blockingHash.hset(key, "one", HELLO);
        assertThat(blockingHash.hexists(key, "one")).isTrue();
    }

    @Test
    void hget() {
        assertThat(blockingHash.hget(key, "one")).isNull();
        blockingHash.hset(key, "one", HELLO);
        assertThat(blockingHash.hget(key, "one")).isEqualTo(HELLO);
    }

    @Test
    void hgetall() {
        assertThat(blockingHash.hgetall(key).isEmpty()).isTrue();

        blockingHash.hset(key, "zero", OTHER);
        blockingHash.hset(key, "one", HELLO);
        blockingHash.hset(key, "two", WORLD);

        Map<String, String> map = blockingHash.hgetall(key);

        assertThat(map).hasSize(3);
        assertThat(map.keySet()).containsExactlyInAnyOrder("zero", "one", "two");
        assertThat(map.values()).containsExactlyInAnyOrder(OTHER, HELLO, WORLD);

        assertThat(blockingHash.hgetall("missing")).isEmpty();
    }

    @Test
    void hincrby() {
        assertThat(blockingHash.hincrby(key, "one", 1)).isEqualTo(1);
        assertThat(blockingHash.hincrby(key, "one", -2)).isEqualTo(-1);
    }

    @Test
    void hincrbyfloat() {
        assertThat(blockingHash.hincrbyfloat(key, "one", 1.0)).isEqualTo(1.0);
        assertThat(blockingHash.hincrbyfloat(key, "one", -2.0)).isEqualTo(-1.0);
        assertThat(blockingHash.hincrbyfloat(key, "one", 1.23)).isEqualTo(0.23, offset(0.001));
    }

    @Test
    void hkeys() {
        populate();
        List<String> keys = blockingHash.hkeys(key);
        assertThat(keys).hasSize(2);
        assertThat(keys).containsExactly("one", "two");
    }

    private void populate() {
        assertThat(blockingHash.hkeys(key)).isEqualTo(Collections.emptyList());
        blockingHash.hset(key, "one", HELLO);
        blockingHash.hset(key, "two", WORLD);
    }

    @Test
    void hlen() {
        assertThat(blockingHash.hlen(key)).isEqualTo(0);
        blockingHash.hset(key, "one", HELLO);
        assertThat(blockingHash.hlen(key)).isEqualTo(1);
    }

    @Test
    void hstrlen() {
        assertThat(blockingHash.hstrlen(key, "one")).isEqualTo(0);
        blockingHash.hset(key, "one", HELLO);
        assertThat(blockingHash.hstrlen(key, "one")).isEqualTo(HELLO.length());
    }

    @Test
    void hmget() {
        populateForHmget();
        Map<String, String> values = blockingHash.hmget(key, "one", "missing", "two");
        assertThat(values).hasSize(3);
        assertThat(values).containsExactly(entry("one", HELLO), entry("missing", null), entry("two", WORLD));
    }

    private void populateForHmget() {
        assertThat(blockingHash.hmget(key, "one", "two")).allSatisfy((f, v) -> assertThat(v).isNull());
        blockingHash.hset(key, "one", HELLO);
        blockingHash.hset(key, "two", WORLD);
    }

    @Test
    void hmset() {
        blockingHash.hmset(key, Map.of("one", HELLO, "two", WORLD));
        assertThat(blockingHash.hmget(key, "one", "two")).containsExactly(entry("one", HELLO), entry("two", WORLD));
    }

    @Test
    void hmsetWithNulls() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("one", null);
        blockingHash.hmset(key, map);
        assertThat(blockingHash.hmget(key, "one")).containsExactly(entry("one", ""));

        map.put("one", HELLO);
        blockingHash.hmset(key, map);
        assertThat(blockingHash.hmget(key, "one")).containsExactly(entry("one", HELLO));
    }

    @Test
    void hrandfield() {
        blockingHash.hset(key, Map.of("one", HELLO, "two", WORLD, "three", OTHER));

        assertThat(blockingHash.hrandfield(key)).isIn("one", "two", "three");
        assertThat(blockingHash.hrandfield(key, 2)).hasSize(2).containsAnyOf("one", "two", "three");
    }

    @Test
    void hrandfieldWithValues() {
        Map<String, String> map = Map.of("one", HELLO, "two", WORLD, "three", OTHER);
        blockingHash.hset(key, map);

        assertThat(blockingHash.hrandfieldWithValues(key, 1))
                .anySatisfy((f, v) -> assertThat(map.get(f)).isEqualTo(v));
        assertThat(blockingHash.hrandfieldWithValues(key, 2)).hasSize(2)
                .allSatisfy((f, v) -> assertThat(map.get(f)).isEqualTo(v));

        assertThat(blockingHash.hrandfieldWithValues(key, -20)).isNotEmpty();
        assertThat(blockingHash.hrandfieldWithValues(key, 3)).containsExactlyInAnyOrderEntriesOf(map);

        assertThat(blockingHash.hrandfieldWithValues("missing", 3)).isEmpty();
    }

    @Test
    void hset() {
        assertThat(blockingHash.hset(key, "one", HELLO)).isTrue();
        assertThat(blockingHash.hset(key, "one", HELLO)).isFalse();
    }

    @Test
    void hsetMap() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("two", WORLD);
        map.put("three", OTHER);
        assertThat(blockingHash.hset(key, map)).isEqualTo(2);

        map.put("two", WORLD);
        assertThat(blockingHash.hset(key, map)).isEqualTo(0);
        assertThat(blockingHash.hget(key, "two")).isEqualTo(WORLD);
    }

    @Test
    void hsetnx() {
        blockingHash.hset(key, "one", HELLO);
        assertThat(blockingHash.hsetnx(key, "one", WORLD)).isFalse();
        assertThat(blockingHash.hget(key, "one")).isEqualTo(HELLO);
    }

    @Test
    void hvals() {
        assertThat(blockingHash.hvals(key)).isEqualTo(List.of());
        blockingHash.hset(key, "one", HELLO);
        blockingHash.hset(key, "two", WORLD);
        List<String> values = blockingHash.hvals(key);
        assertThat(values).hasSize(2).containsExactly(HELLO, WORLD);
    }

    @Test
    void hscan() {
        blockingHash.hset(key, "one", OTHER);
        HashScanCursor<String, String> cursor = blockingHash.hscan(key);

        assertThat(cursor.hasNext()).isTrue();
        Map<String, String> next = cursor.next();

        assertThat(next).containsExactly(entry("one", OTHER));
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void hscanEmpty() {
        HashScanCursor<String, String> cursor = blockingHash.hscan(key);

        assertThat(cursor.hasNext()).isTrue();
        Map<String, String> next = cursor.next();

        assertThat(cursor.hasNext()).isFalse();
        assertThat(next).isEmpty();
    }

    @Test
    void hscanAsIteratorEmpty() {
        HashScanCursor<String, String> cursor = blockingHash.hscan(key);
        Iterable<Map.Entry<String, String>> iterable = cursor.toIterable();

        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, String> entry : iterable) {
            keys.add(entry.getKey());
        }
        assertThat(keys).isEmpty();
    }

    @Test
    void hscanWithArgs() {
        blockingHash.hset(key, "one", OTHER);
        blockingHash.hset(key, "two", HELLO);
        blockingHash.hset(key, "three", WORLD);
        HashScanCursor<String, String> cursor = blockingHash.hscan(key, new ScanArgs().count(3));

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

        HashScanCursor<String, String> cursor = blockingHash.hscan(key, new ScanArgs().count(5));
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

        HashScanCursor<String, String> cursor = blockingHash.hscan(key, new ScanArgs().count(5));
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

        HashScanCursor<String, String> cursor = blockingHash.hscan(key, new ScanArgs().match("f1*"));
        while (cursor.hasNext()) {
            check.putAll(cursor.next());
        }

        assertThat(check).hasSize(11);
    }

    private void populateManyEntries(Map<String, String> expect) {
        for (int i = 0; i < 100; i++) {
            expect.put("f" + i, "hello" + i);
        }
        blockingHash.hset(key, expect);
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

        ReactiveHashScanCursor<String, String> cursor = reactiveHash.hscan(key, new ScanArgs().count(5));
        List<Map.Entry<String, String>> entries = cursor.toMulti().collect().asList().await().atMost(TIMEOUT);

        Map<String, String> check = new LinkedHashMap<>();
        entries.forEach(e -> check.put(e.getKey(), e.getValue()));
        assertThat(check).isEqualTo(expect);
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void hscanCursorId() {
        blockingHash.hset(key, "one", HELLO);

        ReactiveHashScanCursor<String, String> cursor = reactiveHash.hscan(key);
        assertThat(cursor.cursorId()).isEqualTo(0L);

        cursor.next().await().atMost(TIMEOUT);

        assertThat(cursor.hasNext()).isFalse();
        assertThat(cursor.cursorId()).isEqualTo(0L);
    }

}
