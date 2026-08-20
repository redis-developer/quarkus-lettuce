package io.quarkus.redis.runtime.client.lettuce.set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.ListCommands;
import io.quarkus.redis.datasource.set.ReactiveSScanCursor;
import io.quarkus.redis.datasource.set.ReactiveSetCommands;
import io.quarkus.redis.datasource.set.SScanCursor;
import io.quarkus.redis.datasource.set.SetCommands;
import io.quarkus.redis.runtime.client.lettuce.CommandsTestBase;

/**
 * Integration test for {@link LettuceReactiveSetCommandsImpl} and {@link LettuceBlockingSetCommandsImpl},
 * mirroring the Vert.x backend's {@code SetCommandsTest}.
 */
class LettuceSetCommandsTest extends CommandsTestBase {

    private static final String KEY = "key-set";

    private ReactiveRedisDataSource reactiveDs;
    private RedisDataSource blockingDs;
    private ReactiveSetCommands<String, String> reactive;
    private SetCommands<String, String> blocking;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        blockingDs = blockingDataSource();
        reactive = reactiveDs.set(String.class);
        blocking = blockingDs.set(String.class);
    }

    @Test
    void getDataSource() {
        assertThat(reactive.getDataSource()).isEqualTo(reactiveDs);
        assertThat(blocking.getDataSource()).isEqualTo(blockingDs);
    }

    @Test
    void sadd() {
        assertThat(blocking.sadd(KEY, "a")).isEqualTo(1);
        assertThat(blocking.sadd(KEY, "a")).isEqualTo(0);
        assertThat(blocking.smembers(KEY)).isEqualTo(Set.of("a"));
        assertThat(blocking.sadd(KEY, "b", "c")).isEqualTo(2);
        assertThat(blocking.smembers(KEY)).isEqualTo(Set.of("a", "b", "c"));
    }

    @Test
    void scard() {
        assertThat(blocking.scard(KEY)).isEqualTo(0);
        blocking.sadd(KEY, "a");
        assertThat(blocking.scard(KEY)).isEqualTo(1);
    }

    @Test
    void sdiff() {
        populate();
        assertThat(blocking.sdiff("key1", "key2", "key3")).isEqualTo(Set.of("b", "d"));
    }

    @Test
    void sdiffstore() {
        populate();
        assertThat(blocking.sdiffstore("newset", "key1", "key2", "key3")).isEqualTo(2);
        assertThat(blocking.smembers("newset")).containsOnly("b", "d");
    }

    @Test
    void sinter() {
        populate();
        assertThat(blocking.sinter("key1", "key2", "key3")).isEqualTo(Set.of("c"));
        assertThat(blocking.sintercard("key1", "key2", "key3")).isEqualTo(1);
        assertThat(blocking.sintercard(2, "key1", "key2", "key3")).isEqualTo(1);
    }

    @Test
    void sinterstore() {
        populate();
        assertThat(blocking.sinterstore("newset", "key1", "key2", "key3")).isEqualTo(1);
        assertThat(blocking.smembers("newset")).containsExactly("c");
    }

    @Test
    void sismember() {
        assertThat(blocking.sismember(KEY, "a")).isFalse();
        blocking.sadd(KEY, "a");
        assertThat(blocking.sismember(KEY, "a")).isTrue();
    }

    @Test
    void smembersOnMissingKey() {
        assertThat(blocking.smembers("missing")).isEmpty();
    }

    @Test
    void smismember() {
        assertThat(blocking.smismember(KEY, "a")).isEqualTo(List.of(false));
        blocking.sadd(KEY, "a");
        assertThat(blocking.smismember(KEY, "a")).isEqualTo(List.of(true));
        assertThat(blocking.smismember(KEY, "b", "a")).isEqualTo(List.of(false, true));
    }

    @Test
    void smove() {
        blocking.sadd(KEY, "a", "b", "c");
        assertThat(blocking.smove(KEY, "key1", "d")).isFalse();
        assertThat(blocking.smove(KEY, "key1", "a")).isTrue();
        assertThat(blocking.smembers(KEY)).isEqualTo(Set.of("b", "c"));
        assertThat(blocking.smembers("key1")).isEqualTo(Set.of("a"));
    }

    @Test
    void spop() {
        assertThat(blocking.spop(KEY)).isNull();
        blocking.sadd(KEY, "a", "b", "c");
        String popped = blocking.spop(KEY);
        assertThat(Set.of("a", "b", "c")).contains(popped);
        assertThat(blocking.smembers(KEY)).doesNotContain(popped).hasSize(2);
    }

    @Test
    void spopMultiple() {
        assertThat(blocking.spop(KEY, 2)).isEmpty();
        blocking.sadd(KEY, "a", "b", "c");
        Set<String> popped = blocking.spop(KEY, 2);
        assertThat(popped).hasSize(2);
        assertThat(Set.of("a", "b", "c")).containsAll(popped);
        assertThat(blocking.scard(KEY)).isEqualTo(1);
    }

    @Test
    void srandmember() {
        assertThat(blocking.srandmember(KEY)).isNull();
        assertThat(blocking.srandmember(KEY, 3)).isEmpty();

        blocking.sadd(KEY, "a", "b", "c", "d");
        assertThat(Set.of("a", "b", "c", "d")).contains(blocking.srandmember(KEY));
        assertThat(blocking.smembers(KEY)).isEqualTo(Set.of("a", "b", "c", "d"));

        List<String> picked = blocking.srandmember(KEY, 3);
        assertThat(picked).hasSize(3);
        assertThat(Set.of("a", "b", "c", "d")).containsAll(picked);
    }

    /** A negative count is a legal SRANDMEMBER request for duplicates, so it is not validated. */
    @Test
    void srandmemberWithNegativeCountReturnsDuplicates() {
        blocking.sadd(KEY, "a", "b");
        assertThat(blocking.srandmember(KEY, -10)).hasSize(10);
    }

    @Test
    void srem() {
        blocking.sadd(KEY, "a", "b", "c");
        assertThat(blocking.srem(KEY, "d")).isEqualTo(0);
        assertThat(blocking.srem(KEY, "b")).isEqualTo(1);
        assertThat(blocking.smembers(KEY)).isEqualTo(Set.of("a", "c"));
        assertThat(blocking.srem(KEY, "a", "c")).isEqualTo(2);
        assertThat(blocking.smembers(KEY)).isEmpty();
    }

    @Test
    void sunion() {
        populate();
        assertThat(blocking.sunion("key1", "key2", "key3")).isEqualTo(Set.of("a", "b", "c", "d", "e"));
    }

    @Test
    void sunionstore() {
        populate();
        assertThat(blocking.sunionstore("newset", "key1", "key2", "key3")).isEqualTo(5);
        assertThat(blocking.smembers("newset")).isEqualTo(Set.of("a", "b", "c", "d", "e"));
    }

    @Test
    void sscan() {
        blocking.sadd(KEY, "a");
        SScanCursor<String> cursor = blocking.sscan(KEY);

        assertThat(cursor.hasNext()).isTrue();
        List<String> list = cursor.next();

        assertThat(cursor.hasNext()).isFalse();
        assertThat(cursor.cursorId()).isEqualTo(0);
        assertThat(list).containsExactly("a");
    }

    @Test
    void sscanEmpty() {
        SScanCursor<String> cursor = blocking.sscan(KEY);

        assertThat(cursor.hasNext()).isTrue();
        List<String> list = cursor.next();

        assertThat(cursor.hasNext()).isFalse();
        assertThat(list).isEmpty();
    }

    @Test
    void sscanEmptyAsIterable() {
        SScanCursor<String> cursor = blocking.sscan(KEY);

        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.toIterable()).isEmpty();
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void sscanWithArgs() {
        blocking.sadd(KEY, "a");
        SScanCursor<String> cursor = blocking.sscan(KEY, new ScanArgs().count(3));

        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next()).containsExactly("a");
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void sscanMultiple() {
        Set<String> expected = populateMany();

        Set<String> found = new HashSet<>();
        SScanCursor<String> cursor = blocking.sscan(KEY, new ScanArgs().count(5));
        while (cursor.hasNext()) {
            found.addAll(cursor.next());
        }

        assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void sscanMultipleAsIterable() {
        Set<String> expected = populateMany();

        Set<String> found = new HashSet<>();
        SScanCursor<String> cursor = blocking.sscan(KEY, new ScanArgs().count(5));
        for (String member : cursor.toIterable()) {
            found.add(member);
        }

        assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void sscanMatch() {
        populateMany();

        Set<String> found = new HashSet<>();
        SScanCursor<String> cursor = blocking.sscan(KEY, new ScanArgs().count(200).match("hello1*"));
        while (cursor.hasNext()) {
            found.addAll(cursor.next());
        }

        // hello1 plus hello10..hello19
        assertThat(found).hasSize(11);
    }

    @Test
    void sscanReactiveAsMulti() {
        Set<String> expected = populateMany();

        ReactiveSScanCursor<String> cursor = reactive.sscan(KEY, new ScanArgs().count(5));
        assertThat(cursor.cursorId()).isEqualTo(0);
        assertThat(cursor.hasNext()).isTrue();

        List<String> found = cursor.toMulti().collect().asList().await().atMost(TIMEOUT);

        assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(cursor.hasNext()).isFalse();
        assertThat(cursor.cursorId()).isEqualTo(0);
    }

    @Test
    void sort() {
        blocking.sadd(KEY, "9", "5", "1", "3", "8", "7", "6", "2", "4");
        assertThat(blocking.sort(KEY)).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(blocking.sort(KEY, new SortArgs().descending()))
                .containsExactly("9", "8", "7", "6", "5", "4", "3", "2", "1");

        String alphaKey = KEY + "-alpha";
        blocking.sadd(alphaKey, "a", "e", "f", "b");
        assertThat(blocking.sort(alphaKey, new SortArgs().alpha())).containsExactly("a", "b", "e", "f");
        assertThat(blocking.sort(alphaKey, new SortArgs().alpha().limit(1, 2))).containsExactly("b", "e");
    }

    @Test
    void sortOnMissingKey() {
        assertThat(blocking.sort("missing")).isEmpty();
    }

    @Test
    void sortAndStore() {
        String alphaKey = KEY + "-alpha";
        blocking.sadd(KEY, "9", "5", "1", "3", "8", "7", "6", "2", "4");
        blocking.sadd(alphaKey, "a", "e", "f", "b");

        assertThat(blocking.sortAndStore(alphaKey, "dest1", new SortArgs().alpha())).isEqualTo(4);
        assertThat(blocking.sortAndStore(KEY, "dest2")).isEqualTo(9);

        // SORT ... STORE writes a list, so read the destinations back through the list group.
        ListCommands<String, String> lists = blockingDs.list(String.class);
        assertThat(lists.lrange("dest1", 0, -1)).containsExactly("a", "b", "e", "f");
        assertThat(lists.lrange("dest2", 0, -1))
                .containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void reactiveApi() {
        assertThat(reactive.sadd(KEY, "a", "b", "c").await().atMost(TIMEOUT)).isEqualTo(3);
        assertThat(reactive.scard(KEY).await().atMost(TIMEOUT)).isEqualTo(3L);
        assertThat(reactive.smembers(KEY).await().atMost(TIMEOUT)).containsExactlyInAnyOrder("a", "b", "c");
        assertThat(reactive.sismember(KEY, "a").await().atMost(TIMEOUT)).isTrue();
        assertThat(reactive.smismember(KEY, "a", "z").await().atMost(TIMEOUT)).containsExactly(true, false);
        assertThat(reactive.srandmember(KEY, 2).await().atMost(TIMEOUT)).hasSize(2);
        assertThat(reactive.smove(KEY, "other", "a").await().atMost(TIMEOUT)).isTrue();

        reactive.sadd("other", "d").await().atMost(TIMEOUT);
        assertThat(reactive.sunion(KEY, "other").await().atMost(TIMEOUT)).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(reactive.sdiff(KEY, "other").await().atMost(TIMEOUT)).containsExactlyInAnyOrder("b", "c");
        assertThat(reactive.sinter(KEY, "other").await().atMost(TIMEOUT)).isEmpty();
        assertThat(reactive.sintercard(KEY, "other").await().atMost(TIMEOUT)).isEqualTo(0L);
        assertThat(reactive.sunionstore("dest", KEY, "other").await().atMost(TIMEOUT)).isEqualTo(4L);
        assertThat(reactive.sdiffstore("dest", KEY, "other").await().atMost(TIMEOUT)).isEqualTo(2L);
        assertThat(reactive.sinterstore("dest", KEY, "other").await().atMost(TIMEOUT)).isEqualTo(0L);

        assertThat(reactive.sort(KEY, new SortArgs().alpha()).await().atMost(TIMEOUT)).containsExactly("b", "c");
        assertThat(reactive.sortAndStore(KEY, "sorted", new SortArgs().alpha()).await().atMost(TIMEOUT)).isEqualTo(2L);

        assertThat(reactive.srem(KEY, "b").await().atMost(TIMEOUT)).isEqualTo(1);
        assertThat(reactive.spop(KEY).await().atMost(TIMEOUT)).isEqualTo("c");
        assertThat(reactive.spop(KEY).await().atMost(TIMEOUT)).isNull();
    }

    @Test
    void setWithTypeReference() {
        SetCommands<String, String> memberOnly = blockingDs.set(new TypeReference<>() {
            // Empty on purpose
        });
        assertThat(memberOnly.sadd(KEY, "a", "b")).isEqualTo(2);

        SetCommands<String, String> keyAndMember = blockingDs.set(new TypeReference<>() {
            // Empty on purpose
        }, new TypeReference<>() {
            // Empty on purpose
        });
        assertThat(keyAndMember.smembers(KEY)).containsExactlyInAnyOrder("a", "b");

        ReactiveSetCommands<String, String> reactiveCommands = reactiveDs.set(new TypeReference<>() {
            // Empty on purpose
        });
        assertThat(reactiveCommands.scard(KEY).await().atMost(TIMEOUT)).isEqualTo(2L);
    }

    @Test
    void invalidCountsAreRejected() {
        assertThatThrownBy(() -> blocking.spop(KEY, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blocking.sintercard(0, "key1", "key2"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("limit");
        assertThatThrownBy(() -> blocking.sintercard(-1, "key1", "key2"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("limit");
    }

    /** Building the {@code Uni} is fine; only subscribing surfaces the single-key failure. */
    @Test
    void singleKeyFailureIsDeferredUntilSubscription() {
        var uni = reactive.sdiff("key1");
        assertThatThrownBy(() -> uni.await().atMost(TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at least 2 keys");
    }

    private void populate() {
        blocking.sadd(KEY, "a", "b", "c");
        blocking.sadd("key1", "a", "b", "c", "d");
        blocking.sadd("key2", "c");
        blocking.sadd("key3", "a", "c", "e");
    }

    private Set<String> populateMany() {
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            blocking.sadd(KEY, "hello" + i);
            expected.add("hello" + i);
        }
        return expected;
    }
}
