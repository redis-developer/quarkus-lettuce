package io.quarkus.redis.runtime.client.lettuce.list;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.KeyValue;
import io.quarkus.redis.datasource.list.LPosArgs;
import io.quarkus.redis.datasource.list.ListCommands;
import io.quarkus.redis.datasource.list.Position;
import io.quarkus.redis.datasource.list.ReactiveListCommands;
import io.quarkus.redis.runtime.client.lettuce.CommandsTestBase;

class LettuceListCommandsTest extends CommandsTestBase {

    ReactiveRedisDataSource reactiveDs;
    RedisDataSource blockingDs;
    ReactiveListCommands<String, String> reactiveList;
    ListCommands<String, String> blockingList;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        blockingDs = blockingDataSource();
        reactiveList = reactiveDs.list(String.class);
        blockingList = blockingDs.list(String.class);
    }

    @Test
    void getDataSource() {
        assertThat(reactiveDs).isEqualTo(reactiveList.getDataSource());
        assertThat(blockingDs).isEqualTo(blockingList.getDataSource());
    }

    @Test
    void blpop() {
        blockingList.rpush("two", "v2", "v3");
        assertThat(blockingList.blpop(Duration.ofSeconds(1), "one", "two")).isEqualTo(KeyValue.of("two", "v2"));
    }

    @Test
    void blpopTimeout() {
        assertThat(blockingList.blpop(Duration.ofSeconds(1), key)).isNull();
    }

    @Test
    void blpopWithFractionalTimeout() {
        blockingList.rpush("two", "v2");
        assertThat(blockingList.blpop(Duration.ofMillis(1500), "one", "two")).isEqualTo(KeyValue.of("two", "v2"));
        assertThat(blockingList.blpop(Duration.ofMillis(100), "one", "two")).isNull();
    }

    @Test
    void brpop() {
        blockingList.rpush("two", "v2", "v3");
        assertThat(blockingList.brpop(Duration.ofSeconds(1), "one", "two")).isEqualTo(KeyValue.of("two", "v3"));
    }

    @Test
    void brpopTimeout() {
        assertThat(blockingList.brpop(Duration.ofSeconds(1), key)).isNull();
    }

    @Test
    void blmpop() {
        blockingList.rpush("two", "v1", "v2", "v3");
        assertThat(blockingList.blmpop(Duration.ofSeconds(1), Position.RIGHT, "one", "two"))
                .isEqualTo(KeyValue.of("two", "v3"));
        assertThat(blockingList.blmpop(Duration.ofSeconds(1), Position.LEFT, "one", "two"))
                .isEqualTo(KeyValue.of("two", "v1"));
        assertThat(blockingList.blmpop(Duration.ofSeconds(1), Position.LEFT, "one", "two"))
                .isEqualTo(KeyValue.of("two", "v2"));
        assertThat(blockingList.blmpop(Duration.ofSeconds(1), Position.LEFT, "one", "two")).isNull();
    }

    @Test
    void blmpopMany() {
        blockingList.rpush("two", "v1", "v2", "v3");
        assertThat(blockingList.blmpop(Duration.ofSeconds(1), Position.RIGHT, 2, "one", "two"))
                .containsExactly(KeyValue.of("two", "v3"), KeyValue.of("two", "v2"));
        assertThat(blockingList.blmpop(Duration.ofSeconds(1), Position.RIGHT, 2, "one", "two"))
                .containsExactly(KeyValue.of("two", "v1"));
        assertThat(blockingList.blmpop(Duration.ofSeconds(1), Position.RIGHT, 2, "one", "two")).isEmpty();
    }

    @Test
    void brpoplpush() {
        blockingList.rpush("one", "v1", "v2");
        blockingList.rpush("two", "v3", "v4");
        assertThat(blockingList.brpoplpush(Duration.ofSeconds(1), "one", "two")).isEqualTo("v2");
        assertThat(blockingList.lrange("one", 0, -1)).isEqualTo(List.of("v1"));
        assertThat(blockingList.lrange("two", 0, -1)).isEqualTo(List.of("v2", "v3", "v4"));
    }

    @Test
    void blmove() {
        String list2 = key + "-2";
        blockingList.rpush(key, "v1", "v2", "v3");

        assertThat(blockingList.blmove(key, list2, Position.LEFT, Position.RIGHT, Duration.ofSeconds(1))).isEqualTo("v1");
        assertThat(blockingList.lrange(key, 0, -1)).containsExactly("v2", "v3");
        assertThat(blockingList.lrange(list2, 0, -1)).containsOnly("v1");
    }

    @Test
    void blmoveTimeout() {
        assertThat(blockingList.blmove(key, key + "-2", Position.LEFT, Position.RIGHT, Duration.ofSeconds(1))).isNull();
    }

    @Test
    void lindex() {
        assertThat(blockingList.lindex(key, 0)).isNull();
        blockingList.rpush(key, "v1");
        assertThat(blockingList.lindex(key, 0)).isEqualTo("v1");
    }

    @Test
    void linsertBefore() {
        assertThat(blockingList.linsertBeforePivot(key, "v1", "v2")).isEqualTo(0);
        blockingList.rpush(key, "v1");
        blockingList.rpush(key, "v3");
        assertThat(blockingList.linsertBeforePivot(key, "v3", "v2")).isEqualTo(3);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2", "v3"));
    }

    @Test
    void linsertAfter() {
        assertThat(blockingList.linsertAfterPivot(key, "v1", "v2")).isEqualTo(0);
        blockingList.rpush(key, "v1");
        blockingList.rpush(key, "v3");
        assertThat(blockingList.linsertAfterPivot(key, "v3", "v2")).isEqualTo(3);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v3", "v2"));
    }

    @Test
    void llen() {
        assertThat(blockingList.llen(key)).isEqualTo(0);
        blockingList.lpush(key, "v1");
        assertThat(blockingList.llen(key)).isEqualTo(1);
    }

    @Test
    void lmove() {
        String list2 = key + "-2";
        blockingList.rpush(key, "v1", "v2", "v3");
        assertThat(blockingList.lmove(key, list2, Position.RIGHT, Position.LEFT)).isEqualTo("v3");

        assertThat(blockingList.lrange(key, 0, -1)).containsExactly("v1", "v2");
        assertThat(blockingList.lrange(list2, 0, -1)).containsOnly("v3");
    }

    @Test
    void lmoveOnMissingKey() {
        assertThat(blockingList.lmove(key, key + "-2", Position.RIGHT, Position.LEFT)).isNull();
    }

    @Test
    void lmpop() {
        assertThat(blockingList.lmpop(Position.RIGHT, key)).isNull();
        blockingList.rpush(key, "v1", "v2");
        assertThat(blockingList.lmpop(Position.RIGHT, key)).isEqualTo(KeyValue.of(key, "v2"));
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1"));
    }

    @Test
    void lmpopMany() {
        assertThat(blockingList.lmpop(Position.RIGHT, 2, key)).isEmpty();
        blockingList.rpush(key, "v1", "v2");
        assertThat(blockingList.lmpop(Position.RIGHT, 2, key))
                .containsExactly(KeyValue.of(key, "v2"), KeyValue.of(key, "v1"));
        assertThat(blockingList.lrange(key, 0, -1)).isEmpty();
        assertThat(blockingList.lmpop(Position.RIGHT, 2, key)).isEmpty();
    }

    @Test
    void lpop() {
        assertThat(blockingList.lpop(key)).isNull();
        blockingList.rpush(key, "v1", "v2");
        assertThat(blockingList.lpop(key)).isEqualTo("v1");
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v2"));
    }

    @Test
    void lpopCount() {
        assertThat(blockingList.lpop(key, 1)).isEmpty();
        blockingList.rpush(key, "v1", "v2");
        assertThat(blockingList.lpop(key, 3)).isEqualTo(List.of("v1", "v2"));
    }

    @Test
    void lpos() {
        blockingList.rpush(key, "v4", "v5", "v6", "v1", "v2", "v3", "v6", "v6");

        assertThat(blockingList.lpos("nope", "v4")).isEmpty();
        assertThat(blockingList.lpos(key, "missing")).isEmpty();
        assertThat(blockingList.lpos(key, "v4")).hasValue(0);
        assertThat(blockingList.lpos(key, "v6")).hasValue(2);
        assertThat(blockingList.lpos(key, "v6", new LPosArgs().rank(1))).hasValue(2);
        assertThat(blockingList.lpos(key, "v6", new LPosArgs().rank(2))).hasValue(6);
        assertThat(blockingList.lpos(key, "v6", new LPosArgs().rank(4))).isEmpty();

        assertThat(blockingList.lpos(key, "v6", 0)).containsExactly(2L, 6L, 7L);
        assertThat(blockingList.lpos(key, "v6", 2)).containsExactly(2L, 6L);
        assertThat(blockingList.lpos(key, "v6", 0, new LPosArgs().maxlen(1))).isEmpty();
        assertThat(blockingList.lpos(key, "v6", 0, new LPosArgs().rank(-1))).containsExactly(7L, 6L, 2L);
    }

    @Test
    void lposReactiveReturnsNullWhenAbsent() {
        blockingList.rpush(key, "v1");
        assertThat(reactiveList.lpos(key, "missing").await().atMost(TIMEOUT)).isNull();
        assertThat(reactiveList.lpos(key, "missing", new LPosArgs().rank(1)).await().atMost(TIMEOUT)).isNull();
        assertThat(reactiveList.lpos(key, "v1").await().atMost(TIMEOUT)).isEqualTo(0L);
        assertThat(reactiveList.lpos(key, "missing", 0).await().atMost(TIMEOUT)).isEmpty();
    }

    @Test
    void lpush() {
        assertThat(blockingList.lpush(key, "v2")).isEqualTo(1);
        assertThat(blockingList.lpush(key, "v1")).isEqualTo(2);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2"));
        assertThat(blockingList.lpush(key, "v3", "v4")).isEqualTo(4);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v4", "v3", "v1", "v2"));
    }

    @Test
    void lpushx() {
        assertThat(blockingList.lpushx(key, "v2")).isEqualTo(0);
        blockingList.lpush(key, "v2");
        assertThat(blockingList.lpushx(key, "v1")).isEqualTo(2);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2"));
    }

    @Test
    void lpushxMultiple() {
        assertThat(blockingList.lpushx(key, "v1", "v2")).isEqualTo(0);
        blockingList.lpush(key, "v2");
        assertThat(blockingList.lpushx(key, "v1", "v3")).isEqualTo(3);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v3", "v1", "v2"));
    }

    @Test
    void lrange() {
        assertThat(blockingList.lrange(key, 0, 10)).isEmpty();
        blockingList.rpush(key, "v1", "v2", "v3");
        assertThat(blockingList.lrange(key, 0, 1)).containsExactly("v1", "v2");
        assertThat(blockingList.lrange(key, 0, -1)).hasSize(3);
    }

    @Test
    void lrem() {
        assertThat(blockingList.lrem(key, 0, "v6")).isEqualTo(0);

        blockingList.rpush(key, "v1", "v2", "v1", "v2", "v1");
        assertThat(blockingList.lrem(key, 1, "v1")).isEqualTo(1);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v2", "v1", "v2", "v1"));

        blockingList.lpush(key, "v1");
        assertThat(blockingList.lrem(key, -1, "v1")).isEqualTo(1);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2", "v1", "v2"));

        blockingList.lpush(key, "v1");
        assertThat(blockingList.lrem(key, 0, "v1")).isEqualTo(3);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v2", "v2"));
    }

    @Test
    void lset() {
        blockingList.rpush(key, "v1", "v2", "v3");
        blockingList.lset(key, 2, "v6");
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2", "v6"));
    }

    @Test
    void ltrim() {
        blockingList.rpush(key, "v1", "v2", "v3", "v4", "v5", "v6");
        blockingList.ltrim(key, 0, 3);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2", "v3", "v4"));
        blockingList.ltrim(key, -2, -1);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v3", "v4"));
    }

    @Test
    void rpop() {
        assertThat(blockingList.rpop(key)).isNull();
        blockingList.rpush(key, "v1", "v2");
        assertThat(blockingList.rpop(key)).isEqualTo("v2");
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1"));
    }

    @Test
    void rpopCount() {
        assertThat(blockingList.rpop(key, 1)).isEmpty();
        blockingList.rpush(key, "v1", "v2");
        assertThat(blockingList.rpop(key, 3)).isEqualTo(List.of("v2", "v1"));
    }

    @Test
    void rpoplpush() {
        assertThat(blockingList.rpoplpush("one", "two")).isNull();
        blockingList.rpush("one", "v1", "v2");
        blockingList.rpush("two", "v3", "v4");
        assertThat(blockingList.rpoplpush("one", "two")).isEqualTo("v2");
        assertThat(blockingList.lrange("one", 0, -1)).isEqualTo(List.of("v1"));
        assertThat(blockingList.lrange("two", 0, -1)).isEqualTo(List.of("v2", "v3", "v4"));
    }

    @Test
    void rpush() {
        assertThat(blockingList.rpush(key, "v1")).isEqualTo(1);
        assertThat(blockingList.rpush(key, "v2")).isEqualTo(2);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2"));
        assertThat(blockingList.rpush(key, "v3", "v4")).isEqualTo(4);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2", "v3", "v4"));
    }

    @Test
    void rpushx() {
        assertThat(blockingList.rpushx(key, "v1")).isEqualTo(0);
        blockingList.rpush(key, "v1");
        assertThat(blockingList.rpushx(key, "v2")).isEqualTo(2);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2"));
    }

    @Test
    void rpushxMultiple() {
        assertThat(blockingList.rpushx(key, "v2", "v3")).isEqualTo(0);
        blockingList.rpush(key, "v1");
        assertThat(blockingList.rpushx(key, "v2", "v3")).isEqualTo(3);
        assertThat(blockingList.lrange(key, 0, -1)).isEqualTo(List.of("v1", "v2", "v3"));
    }

    @Test
    void sort() {
        blockingList.rpush(key, "9", "5", "1", "3", "5", "8", "7", "6", "2", "4");

        assertThat(blockingList.sort(key)).containsExactly("1", "2", "3", "4", "5", "5", "6", "7", "8", "9");
        assertThat(blockingList.sort(key, new SortArgs().descending()))
                .containsExactly("9", "8", "7", "6", "5", "5", "4", "3", "2", "1");
        assertThat(blockingList.sort(key, new SortArgs().limit(0, 3)))
                .containsExactly("1", "2", "3");

        String alphaKey = key + "-alpha";
        blockingList.rpush(alphaKey, "a", "e", "f", "b");
        assertThat(blockingList.sort(alphaKey, new SortArgs().alpha())).containsExactly("a", "b", "e", "f");
    }

    @Test
    void sortOnMissingKey() {
        assertThat(blockingList.sort(key)).isEmpty();
    }

    @Test
    void sortAndStore() {
        String alphaKey = key + "-alpha";
        blockingList.rpush(key, "9", "5", "1", "3", "5", "8", "7", "6", "2", "4");
        blockingList.rpush(alphaKey, "a", "e", "f", "b");

        assertThat(blockingList.sortAndStore(alphaKey, "dest1", new SortArgs().alpha())).isEqualTo(4);
        assertThat(blockingList.sortAndStore(key, "dest2")).isEqualTo(10);

        assertThat(blockingList.lpop("dest1", 100)).containsExactly("a", "b", "e", "f");
        assertThat(blockingList.lpop("dest2", 100)).containsExactly("1", "2", "3", "4", "5", "5", "6", "7", "8", "9");
    }

    @Test
    void listWithTypeReference() {
        ListCommands<String, String> commands = blockingDs.list(new TypeReference<String>() {
            // Empty on purpose
        });
        commands.rpush(key, "v1", "v2");
        assertThat(commands.blpop(Duration.ofSeconds(1), "one", key)).isEqualTo(KeyValue.of(key, "v1"));

        ListCommands<String, String> keyAndValue = blockingDs.list(new TypeReference<String>() {
            // Empty on purpose
        }, new TypeReference<String>() {
            // Empty on purpose
        });
        assertThat(keyAndValue.lrange(key, 0, -1)).containsExactly("v2");

        ReactiveListCommands<String, String> reactiveCommands = reactiveDs.list(new TypeReference<String>() {
            // Empty on purpose
        });
        assertThat(reactiveCommands.llen(key).await().atMost(TIMEOUT)).isEqualTo(1L);
    }

    @Test
    void nullTypeArgumentsAreRejected() {
        assertThatThrownBy(() -> blockingDs.list(null, String.class))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("redisKeyType");
        assertThatThrownBy(() -> blockingDs.list(String.class, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("memberType");
        assertThatThrownBy(() -> reactiveDs.list(null, String.class))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("redisKeyType");
        assertThatThrownBy(() -> reactiveDs.list(String.class, (Class<String>) null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("memberType");
    }

    @Test
    void nullKeysAndValuesAreRejected() {
        assertThatThrownBy(() -> blockingList.llen(null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key");
        assertThatThrownBy(() -> blockingList.lindex(null, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key");
        assertThatThrownBy(() -> blockingList.lset(null, 0, "v"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key");
        assertThatThrownBy(() -> blockingList.lset(key, 0, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("element");
        assertThatThrownBy(() -> blockingList.lrem(key, 0, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("element");
        assertThatThrownBy(() -> blockingList.linsertBeforePivot(key, null, "v"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pivot");
        assertThatThrownBy(() -> blockingList.linsertAfterPivot(key, "v", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("element");
        assertThatThrownBy(() -> blockingList.lpos(key, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("element");
        assertThatThrownBy(() -> blockingList.rpoplpush(null, "dest"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("source");
        assertThatThrownBy(() -> blockingList.rpoplpush("src", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("destination");
        assertThatThrownBy(() -> blockingList.lmove(key, "dest", null, Position.LEFT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positionInSource");
        assertThatThrownBy(() -> blockingList.lmove(key, "dest", Position.LEFT, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positionInDest");
        assertThatThrownBy(() -> blockingList.lmpop(null, key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("position");
        assertThatThrownBy(() -> blockingList.sort(key, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sortArguments");
        assertThatThrownBy(() -> blockingList.sortAndStore(key, "dest", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("args");
        assertThatThrownBy(() -> blockingList.sortAndStore(key, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("destination");
    }

    @Test
    void emptyOrNullVarargsAreRejected() {
        assertThatThrownBy(() -> blockingList.lpush(key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("elements");
        assertThatThrownBy(() -> blockingList.lpush(key, "v", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("elements");
        assertThatThrownBy(() -> blockingList.lpushx(key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("elements");
        assertThatThrownBy(() -> blockingList.rpush(key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("values");
        assertThatThrownBy(() -> blockingList.rpushx(key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("values");
        assertThatThrownBy(() -> blockingList.blpop(Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
        assertThatThrownBy(() -> blockingList.brpop(Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
        assertThatThrownBy(() -> blockingList.lmpop(Position.LEFT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
        assertThatThrownBy(() -> blockingList.blmpop(Duration.ofSeconds(1), Position.LEFT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
        assertThatThrownBy(() -> blockingList.blmpop(Duration.ofSeconds(1), Position.LEFT, 1, (String) null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
    }

    @Test
    void invalidCountsAndTimeoutsAreRejected() {
        assertThatThrownBy(() -> blockingList.lpop(key, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blockingList.lpos(key, "v", -1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blockingList.lmpop(Position.LEFT, 0, key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blockingList.blmpop(Duration.ofSeconds(1), Position.LEFT, 0, key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blockingList.blpop(null, key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout");
        assertThatThrownBy(() -> blockingList.blpop(Duration.ofSeconds(-1), key))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout");
        assertThatThrownBy(() -> blockingList.brpoplpush(Duration.ofSeconds(-1), key, "dest"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout");
        assertThatThrownBy(() -> blockingList.blmove(key, "dest", Position.LEFT, Position.RIGHT, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout");
    }

    /** {@code rpop(key, count)} is unvalidated, matching Vert.x: the failure comes from Redis. */
    @Test
    void rpopWithNonPositiveCountFailsOnTheServer() {
        blockingList.rpush(key, "v1", "v2");
        assertThatThrownBy(() -> blockingList.rpop(key, -1))
                .isNotInstanceOf(IllegalArgumentException.class);
        assertThat(blockingList.llen(key)).isEqualTo(2);
    }
}
