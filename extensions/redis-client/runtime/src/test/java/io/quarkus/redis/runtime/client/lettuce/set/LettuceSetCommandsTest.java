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

class LettuceSetCommandsTest extends CommandsTestBase {

    ReactiveRedisDataSource reactiveDs;
    RedisDataSource blockingDs;
    ReactiveSetCommands<String, String> reactiveSet;
    SetCommands<String, String> blockingSet;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        blockingDs = blockingDataSource();
        reactiveSet = reactiveDs.set(String.class);
        blockingSet = blockingDs.set(String.class);
    }

    @Test
    void getDataSource() {
        assertThat(reactiveDs).isEqualTo(reactiveSet.getDataSource());
        assertThat(blockingDs).isEqualTo(blockingSet.getDataSource());
    }

    @Test
    void sadd() {
        assertThat(blockingSet.sadd(key, "a")).isEqualTo(1);
        assertThat(blockingSet.sadd(key, "a")).isEqualTo(0);
        assertThat(blockingSet.smembers(key)).isEqualTo(Set.of("a"));
        assertThat(blockingSet.sadd(key, "b", "c")).isEqualTo(2);
        assertThat(blockingSet.smembers(key)).isEqualTo(Set.of("a", "b", "c"));
    }

    @Test
    void scard() {
        assertThat(blockingSet.scard(key)).isEqualTo(0);
        blockingSet.sadd(key, "a");
        assertThat(blockingSet.scard(key)).isEqualTo(1);
    }

    @Test
    void sdiff() {
        populate();
        assertThat(blockingSet.sdiff("key1", "key2", "key3")).isEqualTo(Set.of("b", "d"));
    }

    @Test
    void sdiffstore() {
        populate();
        assertThat(blockingSet.sdiffstore("newset", "key1", "key2", "key3")).isEqualTo(2);
        assertThat(blockingSet.smembers("newset")).containsOnly("b", "d");
    }

    @Test
    void sinter() {
        populate();
        assertThat(blockingSet.sinter("key1", "key2", "key3")).isEqualTo(Set.of("c"));
        assertThat(blockingSet.sintercard("key1", "key2", "key3")).isEqualTo(1);
        assertThat(blockingSet.sintercard(2, "key1", "key2", "key3")).isEqualTo(1);
    }

    @Test
    void sinterstore() {
        populate();
        assertThat(blockingSet.sinterstore("newset", "key1", "key2", "key3")).isEqualTo(1);
        assertThat(blockingSet.smembers("newset")).containsExactly("c");
    }

    @Test
    void sismember() {
        assertThat(blockingSet.sismember(key, "a")).isFalse();
        blockingSet.sadd(key, "a");
        assertThat(blockingSet.sismember(key, "a")).isTrue();
    }

    @Test
    void smembersOnMissingKey() {
        assertThat(blockingSet.smembers("missing")).isEmpty();
    }

    @Test
    void smismember() {
        assertThat(blockingSet.smismember(key, "a")).isEqualTo(List.of(false));
        blockingSet.sadd(key, "a");
        assertThat(blockingSet.smismember(key, "a")).isEqualTo(List.of(true));
        assertThat(blockingSet.smismember(key, "b", "a")).isEqualTo(List.of(false, true));
    }

    @Test
    void smove() {
        blockingSet.sadd(key, "a", "b", "c");
        assertThat(blockingSet.smove(key, "key1", "d")).isFalse();
        assertThat(blockingSet.smove(key, "key1", "a")).isTrue();
        assertThat(blockingSet.smembers(key)).isEqualTo(Set.of("b", "c"));
        assertThat(blockingSet.smembers("key1")).isEqualTo(Set.of("a"));
    }

    @Test
    void spop() {
        assertThat(blockingSet.spop(key)).isNull();
        blockingSet.sadd(key, "a", "b", "c");
        String popped = blockingSet.spop(key);
        assertThat(Set.of("a", "b", "c")).contains(popped);
        assertThat(blockingSet.smembers(key)).doesNotContain(popped).hasSize(2);
    }

    @Test
    void spopMultiple() {
        assertThat(blockingSet.spop(key, 2)).isEmpty();
        blockingSet.sadd(key, "a", "b", "c");
        Set<String> popped = blockingSet.spop(key, 2);
        assertThat(popped).hasSize(2);
        assertThat(Set.of("a", "b", "c")).containsAll(popped);
        assertThat(blockingSet.scard(key)).isEqualTo(1);
    }

    @Test
    void srandmember() {
        assertThat(blockingSet.srandmember(key)).isNull();
        assertThat(blockingSet.srandmember(key, 3)).isEmpty();

        blockingSet.sadd(key, "a", "b", "c", "d");
        assertThat(Set.of("a", "b", "c", "d")).contains(blockingSet.srandmember(key));
        assertThat(blockingSet.smembers(key)).isEqualTo(Set.of("a", "b", "c", "d"));

        List<String> picked = blockingSet.srandmember(key, 3);
        assertThat(picked).hasSize(3);
        assertThat(Set.of("a", "b", "c", "d")).containsAll(picked);
    }

    /** A negative count is a legal SRANDMEMBER request for duplicates, so it is not validated. */
    @Test
    void srandmemberWithNegativeCountReturnsDuplicates() {
        blockingSet.sadd(key, "a", "b");
        assertThat(blockingSet.srandmember(key, -10)).hasSize(10);
    }

    @Test
    void srem() {
        blockingSet.sadd(key, "a", "b", "c");
        assertThat(blockingSet.srem(key, "d")).isEqualTo(0);
        assertThat(blockingSet.srem(key, "b")).isEqualTo(1);
        assertThat(blockingSet.smembers(key)).isEqualTo(Set.of("a", "c"));
        assertThat(blockingSet.srem(key, "a", "c")).isEqualTo(2);
        assertThat(blockingSet.smembers(key)).isEmpty();
    }

    @Test
    void sunion() {
        populate();
        assertThat(blockingSet.sunion("key1", "key2", "key3")).isEqualTo(Set.of("a", "b", "c", "d", "e"));
    }

    @Test
    void sunionstore() {
        populate();
        assertThat(blockingSet.sunionstore("newset", "key1", "key2", "key3")).isEqualTo(5);
        assertThat(blockingSet.smembers("newset")).isEqualTo(Set.of("a", "b", "c", "d", "e"));
    }

    @Test
    void sscan() {
        blockingSet.sadd(key, "a");
        SScanCursor<String> cursor = blockingSet.sscan(key);

        assertThat(cursor.hasNext()).isTrue();
        List<String> list = cursor.next();

        assertThat(cursor.hasNext()).isFalse();
        assertThat(cursor.cursorId()).isEqualTo(0);
        assertThat(list).containsExactly("a");
    }

    @Test
    void sscanEmpty() {
        SScanCursor<String> cursor = blockingSet.sscan(key);

        assertThat(cursor.hasNext()).isTrue();
        List<String> list = cursor.next();

        assertThat(cursor.hasNext()).isFalse();
        assertThat(list).isEmpty();
    }

    @Test
    void sscanEmptyAsIterable() {
        SScanCursor<String> cursor = blockingSet.sscan(key);

        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.toIterable()).isEmpty();
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void sscanWithArgs() {
        blockingSet.sadd(key, "a");
        SScanCursor<String> cursor = blockingSet.sscan(key, new ScanArgs().count(3));

        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next()).containsExactly("a");
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void sscanMultiple() {
        Set<String> expected = populateMany();

        Set<String> found = new HashSet<>();
        SScanCursor<String> cursor = blockingSet.sscan(key, new ScanArgs().count(5));
        while (cursor.hasNext()) {
            found.addAll(cursor.next());
        }

        assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void sscanMultipleAsIterable() {
        Set<String> expected = populateMany();

        Set<String> found = new HashSet<>();
        SScanCursor<String> cursor = blockingSet.sscan(key, new ScanArgs().count(5));
        for (String member : cursor.toIterable()) {
            found.add(member);
        }

        assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void sscanMatch() {
        populateMany();

        Set<String> found = new HashSet<>();
        SScanCursor<String> cursor = blockingSet.sscan(key, new ScanArgs().count(200).match("hello1*"));
        while (cursor.hasNext()) {
            found.addAll(cursor.next());
        }

        // hello1 plus hello10..hello19
        assertThat(found).hasSize(11);
    }

    @Test
    void sscanReactiveAsMulti() {
        Set<String> expected = populateMany();

        ReactiveSScanCursor<String> cursor = reactiveSet.sscan(key, new ScanArgs().count(5));
        assertThat(cursor.cursorId()).isEqualTo(0);
        assertThat(cursor.hasNext()).isTrue();

        List<String> found = cursor.toMulti().collect().asList().await().atMost(TIMEOUT);

        assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(cursor.hasNext()).isFalse();
        assertThat(cursor.cursorId()).isEqualTo(0);
    }

    @Test
    void sort() {
        blockingSet.sadd(key, "9", "5", "1", "3", "8", "7", "6", "2", "4");
        assertThat(blockingSet.sort(key)).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(blockingSet.sort(key, new SortArgs().descending()))
                .containsExactly("9", "8", "7", "6", "5", "4", "3", "2", "1");

        String alphaKey = key + "-alpha";
        blockingSet.sadd(alphaKey, "a", "e", "f", "b");
        assertThat(blockingSet.sort(alphaKey, new SortArgs().alpha())).containsExactly("a", "b", "e", "f");
        assertThat(blockingSet.sort(alphaKey, new SortArgs().alpha().limit(1, 2))).containsExactly("b", "e");
    }

    @Test
    void sortOnMissingKey() {
        assertThat(blockingSet.sort("missing")).isEmpty();
    }

    @Test
    void sortAndStore() {
        String alphaKey = key + "-alpha";
        blockingSet.sadd(key, "9", "5", "1", "3", "8", "7", "6", "2", "4");
        blockingSet.sadd(alphaKey, "a", "e", "f", "b");

        assertThat(blockingSet.sortAndStore(alphaKey, "dest1", new SortArgs().alpha())).isEqualTo(4);
        assertThat(blockingSet.sortAndStore(key, "dest2")).isEqualTo(9);

        // SORT ... STORE writes a list, so read the destinations back through the list group.
        ListCommands<String, String> lists = blockingDs.list(String.class);
        assertThat(lists.lrange("dest1", 0, -1)).containsExactly("a", "b", "e", "f");
        assertThat(lists.lrange("dest2", 0, -1))
                .containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void setWithTypeReference() {
        SetCommands<String, String> memberOnly = blockingDs.set(new TypeReference<>() {
            // Empty on purpose
        });
        assertThat(memberOnly.sadd(key, "a", "b")).isEqualTo(2);

        SetCommands<String, String> keyAndMember = blockingDs.set(new TypeReference<>() {
            // Empty on purpose
        }, new TypeReference<>() {
            // Empty on purpose
        });
        assertThat(keyAndMember.smembers(key)).containsExactlyInAnyOrder("a", "b");

        ReactiveSetCommands<String, String> reactiveCommands = reactiveDs.set(new TypeReference<>() {
            // Empty on purpose
        });
        assertThat(reactiveCommands.scard(key).await().atMost(TIMEOUT)).isEqualTo(2L);
    }

    @Test
    void invalidCountsAreRejected() {
        assertThatThrownBy(() -> blockingSet.spop(key, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blockingSet.sintercard(0, "key1", "key2"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("limit");
        assertThatThrownBy(() -> blockingSet.sintercard(-1, "key1", "key2"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("limit");
    }

    /** Building the {@code Uni} is fine; only subscribing surfaces the single-key failure. */
    @Test
    void singleKeyFailureIsDeferredUntilSubscription() {
        var uni = reactiveSet.sdiff("key1");
        assertThatThrownBy(() -> uni.await().atMost(TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at least 2 keys");
    }

    private void populate() {
        blockingSet.sadd(key, "a", "b", "c");
        blockingSet.sadd("key1", "a", "b", "c", "d");
        blockingSet.sadd("key2", "c");
        blockingSet.sadd("key3", "a", "c", "e");
    }

    private Set<String> populateMany() {
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            blockingSet.sadd(key, "hello" + i);
            expected.add("hello" + i);
        }
        return expected;
    }
}
