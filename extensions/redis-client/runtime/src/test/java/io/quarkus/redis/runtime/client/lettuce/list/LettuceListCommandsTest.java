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

/**
 * Integration test for {@link LettuceReactiveListCommandsImpl} and {@link LettuceBlockingListCommandsImpl},
 * mirroring the Vert.x backend's {@code ListCommandTest}.
 */
class LettuceListCommandsTest extends CommandsTestBase {

    private static final String KEY = "key-list";

    private ReactiveRedisDataSource reactiveDs;
    private RedisDataSource blockingDs;
    private ReactiveListCommands<String, String> reactive;
    private ListCommands<String, String> blocking;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        blockingDs = blockingDataSource();
        reactive = reactiveDs.list(String.class);
        blocking = blockingDs.list(String.class);
    }

    @Test
    void getDataSource() {
        assertThat(reactive.getDataSource()).isEqualTo(reactiveDs);
        assertThat(blocking.getDataSource()).isEqualTo(blockingDs);
    }

    @Test
    void blpop() {
        blocking.rpush("two", "v2", "v3");
        assertThat(blocking.blpop(Duration.ofSeconds(1), "one", "two")).isEqualTo(KeyValue.of("two", "v2"));
    }

    @Test
    void blpopTimeout() {
        assertThat(blocking.blpop(Duration.ofSeconds(1), KEY)).isNull();
    }

    @Test
    void blpopWithFractionalTimeout() {
        blocking.rpush("two", "v2");
        assertThat(blocking.blpop(Duration.ofMillis(1500), "one", "two")).isEqualTo(KeyValue.of("two", "v2"));
        assertThat(blocking.blpop(Duration.ofMillis(100), "one", "two")).isNull();
    }

    @Test
    void brpop() {
        blocking.rpush("two", "v2", "v3");
        assertThat(blocking.brpop(Duration.ofSeconds(1), "one", "two")).isEqualTo(KeyValue.of("two", "v3"));
    }

    @Test
    void brpopTimeout() {
        assertThat(blocking.brpop(Duration.ofSeconds(1), KEY)).isNull();
    }

    @Test
    void blmpop() {
        blocking.rpush("two", "v1", "v2", "v3");
        assertThat(blocking.blmpop(Duration.ofSeconds(1), Position.RIGHT, "one", "two"))
                .isEqualTo(KeyValue.of("two", "v3"));
        assertThat(blocking.blmpop(Duration.ofSeconds(1), Position.LEFT, "one", "two"))
                .isEqualTo(KeyValue.of("two", "v1"));
        assertThat(blocking.blmpop(Duration.ofSeconds(1), Position.LEFT, "one", "two"))
                .isEqualTo(KeyValue.of("two", "v2"));
        assertThat(blocking.blmpop(Duration.ofSeconds(1), Position.LEFT, "one", "two")).isNull();
    }

    @Test
    void blmpopMany() {
        blocking.rpush("two", "v1", "v2", "v3");
        assertThat(blocking.blmpop(Duration.ofSeconds(1), Position.RIGHT, 2, "one", "two"))
                .containsExactly(KeyValue.of("two", "v3"), KeyValue.of("two", "v2"));
        assertThat(blocking.blmpop(Duration.ofSeconds(1), Position.RIGHT, 2, "one", "two"))
                .containsExactly(KeyValue.of("two", "v1"));
        assertThat(blocking.blmpop(Duration.ofSeconds(1), Position.RIGHT, 2, "one", "two")).isEmpty();
    }

    @Test
    void brpoplpush() {
        blocking.rpush("one", "v1", "v2");
        blocking.rpush("two", "v3", "v4");
        assertThat(blocking.brpoplpush(Duration.ofSeconds(1), "one", "two")).isEqualTo("v2");
        assertThat(blocking.lrange("one", 0, -1)).isEqualTo(List.of("v1"));
        assertThat(blocking.lrange("two", 0, -1)).isEqualTo(List.of("v2", "v3", "v4"));
    }

    @Test
    void blmove() {
        String list2 = KEY + "-2";
        blocking.rpush(KEY, "v1", "v2", "v3");

        assertThat(blocking.blmove(KEY, list2, Position.LEFT, Position.RIGHT, Duration.ofSeconds(1))).isEqualTo("v1");
        assertThat(blocking.lrange(KEY, 0, -1)).containsExactly("v2", "v3");
        assertThat(blocking.lrange(list2, 0, -1)).containsOnly("v1");
    }

    @Test
    void blmoveTimeout() {
        assertThat(blocking.blmove(KEY, KEY + "-2", Position.LEFT, Position.RIGHT, Duration.ofSeconds(1))).isNull();
    }

    @Test
    void lindex() {
        assertThat(blocking.lindex(KEY, 0)).isNull();
        blocking.rpush(KEY, "v1");
        assertThat(blocking.lindex(KEY, 0)).isEqualTo("v1");
    }

    @Test
    void linsertBefore() {
        assertThat(blocking.linsertBeforePivot(KEY, "v1", "v2")).isEqualTo(0);
        blocking.rpush(KEY, "v1");
        blocking.rpush(KEY, "v3");
        assertThat(blocking.linsertBeforePivot(KEY, "v3", "v2")).isEqualTo(3);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2", "v3"));
    }

    @Test
    void linsertAfter() {
        assertThat(blocking.linsertAfterPivot(KEY, "v1", "v2")).isEqualTo(0);
        blocking.rpush(KEY, "v1");
        blocking.rpush(KEY, "v3");
        assertThat(blocking.linsertAfterPivot(KEY, "v3", "v2")).isEqualTo(3);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v3", "v2"));
    }

    @Test
    void llen() {
        assertThat(blocking.llen(KEY)).isEqualTo(0);
        blocking.lpush(KEY, "v1");
        assertThat(blocking.llen(KEY)).isEqualTo(1);
    }

    @Test
    void lmove() {
        String list2 = KEY + "-2";
        blocking.rpush(KEY, "v1", "v2", "v3");
        assertThat(blocking.lmove(KEY, list2, Position.RIGHT, Position.LEFT)).isEqualTo("v3");

        assertThat(blocking.lrange(KEY, 0, -1)).containsExactly("v1", "v2");
        assertThat(blocking.lrange(list2, 0, -1)).containsOnly("v3");
    }

    @Test
    void lmoveOnMissingKey() {
        assertThat(blocking.lmove(KEY, KEY + "-2", Position.RIGHT, Position.LEFT)).isNull();
    }

    @Test
    void lmpop() {
        assertThat(blocking.lmpop(Position.RIGHT, KEY)).isNull();
        blocking.rpush(KEY, "v1", "v2");
        assertThat(blocking.lmpop(Position.RIGHT, KEY)).isEqualTo(KeyValue.of(KEY, "v2"));
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1"));
    }

    @Test
    void lmpopMany() {
        assertThat(blocking.lmpop(Position.RIGHT, 2, KEY)).isEmpty();
        blocking.rpush(KEY, "v1", "v2");
        assertThat(blocking.lmpop(Position.RIGHT, 2, KEY))
                .containsExactly(KeyValue.of(KEY, "v2"), KeyValue.of(KEY, "v1"));
        assertThat(blocking.lrange(KEY, 0, -1)).isEmpty();
        assertThat(blocking.lmpop(Position.RIGHT, 2, KEY)).isEmpty();
    }

    @Test
    void lpop() {
        assertThat(blocking.lpop(KEY)).isNull();
        blocking.rpush(KEY, "v1", "v2");
        assertThat(blocking.lpop(KEY)).isEqualTo("v1");
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v2"));
    }

    @Test
    void lpopCount() {
        assertThat(blocking.lpop(KEY, 1)).isEmpty();
        blocking.rpush(KEY, "v1", "v2");
        assertThat(blocking.lpop(KEY, 3)).isEqualTo(List.of("v1", "v2"));
    }

    @Test
    void lpos() {
        blocking.rpush(KEY, "v4", "v5", "v6", "v1", "v2", "v3", "v6", "v6");

        assertThat(blocking.lpos("nope", "v4")).isEmpty();
        assertThat(blocking.lpos(KEY, "missing")).isEmpty();
        assertThat(blocking.lpos(KEY, "v4")).hasValue(0);
        assertThat(blocking.lpos(KEY, "v6")).hasValue(2);
        assertThat(blocking.lpos(KEY, "v6", new LPosArgs().rank(1))).hasValue(2);
        assertThat(blocking.lpos(KEY, "v6", new LPosArgs().rank(2))).hasValue(6);
        assertThat(blocking.lpos(KEY, "v6", new LPosArgs().rank(4))).isEmpty();

        assertThat(blocking.lpos(KEY, "v6", 0)).containsExactly(2L, 6L, 7L);
        assertThat(blocking.lpos(KEY, "v6", 2)).containsExactly(2L, 6L);
        assertThat(blocking.lpos(KEY, "v6", 0, new LPosArgs().maxlen(1))).isEmpty();
        assertThat(blocking.lpos(KEY, "v6", 0, new LPosArgs().rank(-1))).containsExactly(7L, 6L, 2L);
    }

    @Test
    void lposReactiveReturnsNullWhenAbsent() {
        blocking.rpush(KEY, "v1");
        assertThat(reactive.lpos(KEY, "missing").await().atMost(TIMEOUT)).isNull();
        assertThat(reactive.lpos(KEY, "missing", new LPosArgs().rank(1)).await().atMost(TIMEOUT)).isNull();
        assertThat(reactive.lpos(KEY, "v1").await().atMost(TIMEOUT)).isEqualTo(0L);
        assertThat(reactive.lpos(KEY, "missing", 0).await().atMost(TIMEOUT)).isEmpty();
    }

    @Test
    void lpush() {
        assertThat(blocking.lpush(KEY, "v2")).isEqualTo(1);
        assertThat(blocking.lpush(KEY, "v1")).isEqualTo(2);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2"));
        assertThat(blocking.lpush(KEY, "v3", "v4")).isEqualTo(4);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v4", "v3", "v1", "v2"));
    }

    @Test
    void lpushx() {
        assertThat(blocking.lpushx(KEY, "v2")).isEqualTo(0);
        blocking.lpush(KEY, "v2");
        assertThat(blocking.lpushx(KEY, "v1")).isEqualTo(2);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2"));
    }

    @Test
    void lpushxMultiple() {
        assertThat(blocking.lpushx(KEY, "v1", "v2")).isEqualTo(0);
        blocking.lpush(KEY, "v2");
        assertThat(blocking.lpushx(KEY, "v1", "v3")).isEqualTo(3);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v3", "v1", "v2"));
    }

    @Test
    void lrange() {
        assertThat(blocking.lrange(KEY, 0, 10)).isEmpty();
        blocking.rpush(KEY, "v1", "v2", "v3");
        assertThat(blocking.lrange(KEY, 0, 1)).containsExactly("v1", "v2");
        assertThat(blocking.lrange(KEY, 0, -1)).hasSize(3);
    }

    @Test
    void lrem() {
        assertThat(blocking.lrem(KEY, 0, "v6")).isEqualTo(0);

        blocking.rpush(KEY, "v1", "v2", "v1", "v2", "v1");
        assertThat(blocking.lrem(KEY, 1, "v1")).isEqualTo(1);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v2", "v1", "v2", "v1"));

        blocking.lpush(KEY, "v1");
        assertThat(blocking.lrem(KEY, -1, "v1")).isEqualTo(1);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2", "v1", "v2"));

        blocking.lpush(KEY, "v1");
        assertThat(blocking.lrem(KEY, 0, "v1")).isEqualTo(3);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v2", "v2"));
    }

    @Test
    void lset() {
        blocking.rpush(KEY, "v1", "v2", "v3");
        blocking.lset(KEY, 2, "v6");
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2", "v6"));
    }

    @Test
    void ltrim() {
        blocking.rpush(KEY, "v1", "v2", "v3", "v4", "v5", "v6");
        blocking.ltrim(KEY, 0, 3);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2", "v3", "v4"));
        blocking.ltrim(KEY, -2, -1);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v3", "v4"));
    }

    @Test
    void rpop() {
        assertThat(blocking.rpop(KEY)).isNull();
        blocking.rpush(KEY, "v1", "v2");
        assertThat(blocking.rpop(KEY)).isEqualTo("v2");
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1"));
    }

    @Test
    void rpopCount() {
        assertThat(blocking.rpop(KEY, 1)).isEmpty();
        blocking.rpush(KEY, "v1", "v2");
        assertThat(blocking.rpop(KEY, 3)).isEqualTo(List.of("v2", "v1"));
    }

    @Test
    void rpoplpush() {
        assertThat(blocking.rpoplpush("one", "two")).isNull();
        blocking.rpush("one", "v1", "v2");
        blocking.rpush("two", "v3", "v4");
        assertThat(blocking.rpoplpush("one", "two")).isEqualTo("v2");
        assertThat(blocking.lrange("one", 0, -1)).isEqualTo(List.of("v1"));
        assertThat(blocking.lrange("two", 0, -1)).isEqualTo(List.of("v2", "v3", "v4"));
    }

    @Test
    void rpush() {
        assertThat(blocking.rpush(KEY, "v1")).isEqualTo(1);
        assertThat(blocking.rpush(KEY, "v2")).isEqualTo(2);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2"));
        assertThat(blocking.rpush(KEY, "v3", "v4")).isEqualTo(4);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2", "v3", "v4"));
    }

    @Test
    void rpushx() {
        assertThat(blocking.rpushx(KEY, "v1")).isEqualTo(0);
        blocking.rpush(KEY, "v1");
        assertThat(blocking.rpushx(KEY, "v2")).isEqualTo(2);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2"));
    }

    @Test
    void rpushxMultiple() {
        assertThat(blocking.rpushx(KEY, "v2", "v3")).isEqualTo(0);
        blocking.rpush(KEY, "v1");
        assertThat(blocking.rpushx(KEY, "v2", "v3")).isEqualTo(3);
        assertThat(blocking.lrange(KEY, 0, -1)).isEqualTo(List.of("v1", "v2", "v3"));
    }

    @Test
    void sort() {
        blocking.rpush(KEY, "9", "5", "1", "3", "5", "8", "7", "6", "2", "4");

        assertThat(blocking.sort(KEY)).containsExactly("1", "2", "3", "4", "5", "5", "6", "7", "8", "9");
        assertThat(blocking.sort(KEY, new SortArgs().descending()))
                .containsExactly("9", "8", "7", "6", "5", "5", "4", "3", "2", "1");
        assertThat(blocking.sort(KEY, new SortArgs().limit(0, 3)))
                .containsExactly("1", "2", "3");

        String alphaKey = KEY + "-alpha";
        blocking.rpush(alphaKey, "a", "e", "f", "b");
        assertThat(blocking.sort(alphaKey, new SortArgs().alpha())).containsExactly("a", "b", "e", "f");
    }

    @Test
    void sortOnMissingKey() {
        assertThat(blocking.sort(KEY)).isEmpty();
    }

    @Test
    void sortAndStore() {
        String alphaKey = KEY + "-alpha";
        blocking.rpush(KEY, "9", "5", "1", "3", "5", "8", "7", "6", "2", "4");
        blocking.rpush(alphaKey, "a", "e", "f", "b");

        assertThat(blocking.sortAndStore(alphaKey, "dest1", new SortArgs().alpha())).isEqualTo(4);
        assertThat(blocking.sortAndStore(KEY, "dest2")).isEqualTo(10);

        assertThat(blocking.lpop("dest1", 100)).containsExactly("a", "b", "e", "f");
        assertThat(blocking.lpop("dest2", 100)).containsExactly("1", "2", "3", "4", "5", "5", "6", "7", "8", "9");
    }

    @Test
    void reactiveApi() {
        assertThat(reactive.rpush(KEY, "v1", "v2", "v3").await().atMost(TIMEOUT)).isEqualTo(3L);
        assertThat(reactive.llen(KEY).await().atMost(TIMEOUT)).isEqualTo(3L);
        assertThat(reactive.lrange(KEY, 0, -1).await().atMost(TIMEOUT)).containsExactly("v1", "v2", "v3");
        assertThat(reactive.lindex(KEY, 1).await().atMost(TIMEOUT)).isEqualTo("v2");
        assertThat(reactive.lpop(KEY).await().atMost(TIMEOUT)).isEqualTo("v1");
        assertThat(reactive.rpop(KEY, 2).await().atMost(TIMEOUT)).containsExactly("v3", "v2");
        assertThat(reactive.lpop(KEY).await().atMost(TIMEOUT)).isNull();

        reactive.rpush(KEY, "b", "a", "c").await().atMost(TIMEOUT);
        assertThat(reactive.sort(KEY, new SortArgs().alpha()).await().atMost(TIMEOUT)).containsExactly("a", "b", "c");
        assertThat(reactive.sortAndStore(KEY, "dest", new SortArgs().alpha()).await().atMost(TIMEOUT)).isEqualTo(3L);

        reactive.lset(KEY, 0, "z").await().atMost(TIMEOUT);
        assertThat(reactive.lindex(KEY, 0).await().atMost(TIMEOUT)).isEqualTo("z");
        reactive.ltrim(KEY, 0, 0).await().atMost(TIMEOUT);
        assertThat(reactive.llen(KEY).await().atMost(TIMEOUT)).isEqualTo(1L);

        assertThat(reactive.blmpop(Duration.ofSeconds(1), Position.LEFT, KEY).await().atMost(TIMEOUT))
                .isEqualTo(KeyValue.of(KEY, "z"));
    }

    @Test
    void listWithTypeReference() {
        ListCommands<String, String> commands = blockingDs.list(new TypeReference<String>() {
            // Empty on purpose
        });
        commands.rpush(KEY, "v1", "v2");
        assertThat(commands.blpop(Duration.ofSeconds(1), "one", KEY)).isEqualTo(KeyValue.of(KEY, "v1"));

        ListCommands<String, String> keyAndValue = blockingDs.list(new TypeReference<String>() {
            // Empty on purpose
        }, new TypeReference<String>() {
            // Empty on purpose
        });
        assertThat(keyAndValue.lrange(KEY, 0, -1)).containsExactly("v2");

        ReactiveListCommands<String, String> reactiveCommands = reactiveDs.list(new TypeReference<String>() {
            // Empty on purpose
        });
        assertThat(reactiveCommands.llen(KEY).await().atMost(TIMEOUT)).isEqualTo(1L);
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
        assertThatThrownBy(() -> blocking.llen(null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key");
        assertThatThrownBy(() -> blocking.lindex(null, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key");
        assertThatThrownBy(() -> blocking.lset(null, 0, "v"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key");
        assertThatThrownBy(() -> blocking.lset(KEY, 0, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("element");
        assertThatThrownBy(() -> blocking.lrem(KEY, 0, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("element");
        assertThatThrownBy(() -> blocking.linsertBeforePivot(KEY, null, "v"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pivot");
        assertThatThrownBy(() -> blocking.linsertAfterPivot(KEY, "v", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("element");
        assertThatThrownBy(() -> blocking.lpos(KEY, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("element");
        assertThatThrownBy(() -> blocking.rpoplpush(null, "dest"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("source");
        assertThatThrownBy(() -> blocking.rpoplpush("src", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("destination");
        assertThatThrownBy(() -> blocking.lmove(KEY, "dest", null, Position.LEFT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positionInSource");
        assertThatThrownBy(() -> blocking.lmove(KEY, "dest", Position.LEFT, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positionInDest");
        assertThatThrownBy(() -> blocking.lmpop(null, KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("position");
        assertThatThrownBy(() -> blocking.sort(KEY, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sortArguments");
        assertThatThrownBy(() -> blocking.sortAndStore(KEY, "dest", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("args");
        assertThatThrownBy(() -> blocking.sortAndStore(KEY, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("destination");
    }

    @Test
    void emptyOrNullVarargsAreRejected() {
        assertThatThrownBy(() -> blocking.lpush(KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("elements");
        assertThatThrownBy(() -> blocking.lpush(KEY, "v", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("elements");
        assertThatThrownBy(() -> blocking.lpushx(KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("elements");
        assertThatThrownBy(() -> blocking.rpush(KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("values");
        assertThatThrownBy(() -> blocking.rpushx(KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("values");
        assertThatThrownBy(() -> blocking.blpop(Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
        assertThatThrownBy(() -> blocking.brpop(Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
        assertThatThrownBy(() -> blocking.lmpop(Position.LEFT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
        assertThatThrownBy(() -> blocking.blmpop(Duration.ofSeconds(1), Position.LEFT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
        assertThatThrownBy(() -> blocking.blmpop(Duration.ofSeconds(1), Position.LEFT, 1, (String) null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keys");
    }

    @Test
    void invalidCountsAndTimeoutsAreRejected() {
        assertThatThrownBy(() -> blocking.lpop(KEY, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blocking.lpos(KEY, "v", -1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blocking.lmpop(Position.LEFT, 0, KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blocking.blmpop(Duration.ofSeconds(1), Position.LEFT, 0, KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("count");
        assertThatThrownBy(() -> blocking.blpop(null, KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout");
        assertThatThrownBy(() -> blocking.blpop(Duration.ofSeconds(-1), KEY))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout");
        assertThatThrownBy(() -> blocking.brpoplpush(Duration.ofSeconds(-1), KEY, "dest"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout");
        assertThatThrownBy(() -> blocking.blmove(KEY, "dest", Position.LEFT, Position.RIGHT, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("timeout");
    }

    /** {@code rpop(key, count)} is unvalidated, matching Vert.x: the failure comes from Redis. */
    @Test
    void rpopWithNonPositiveCountFailsOnTheServer() {
        blocking.rpush(KEY, "v1", "v2");
        assertThatThrownBy(() -> blocking.rpop(KEY, -1))
                .isNotInstanceOf(IllegalArgumentException.class);
        assertThat(blocking.llen(KEY)).isEqualTo(2);
    }
}
