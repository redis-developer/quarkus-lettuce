package io.quarkus.redis.runtime.client.lettuce.set;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.set.ReactiveTransactionalSetCommands;
import io.quarkus.redis.datasource.set.TransactionalSetCommands;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.runtime.client.lettuce.CommandsTestBase;

class LettuceTransactionalSetCommandsTest extends CommandsTestBase {

    private static final String KEY = "tx-set-key";
    private static final String OTHER = "tx-set-other";

    private RedisDataSource blockingDs;
    private ReactiveRedisDataSource reactiveDs;

    @BeforeEach
    void initialize() {
        blockingDs = blockingDataSource(Duration.ofSeconds(60));
        reactiveDs = reactiveDataSource();
        // SUNION needs a second, non-watched set; populate it before the transaction opens.
        blockingDs.set(String.class).sadd(OTHER, "x");
    }

    @Test
    void setBlocking() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalSetCommands<String, String> set = tx.set(String.class);
            assertThat(set.getDataSource()).isEqualTo(tx);
            set.sadd(KEY, "a", "b", "c", "d"); // 0 -> 4
            set.scard(KEY); // 1 -> 4
            set.smembers(KEY); // 2 -> [a, b, c, d]
            set.sismember(KEY, "a"); // 3 -> true
            set.smismember(KEY, "a", "z"); // 4 -> [true, false]
            set.srandmember(KEY, 2); // 5 -> two members
            set.srem(KEY, "a"); // 6 -> 1
            set.sunion(KEY, OTHER); // 7 -> [b, c, d, x]
            set.spop(KEY); // 8 -> one of b, c, d
        });

        assertResult(result);
    }

    @Test
    void setBlockingWithWatch() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalSetCommands<String, String> set = tx.set(String.class);
            set.sadd(KEY, "a", "b", "c", "d");
            set.scard(KEY);
            set.smembers(KEY);
            set.sismember(KEY, "a");
            set.smismember(KEY, "a", "z");
            set.srandmember(KEY, 2);
            set.srem(KEY, "a");
            set.sunion(KEY, OTHER);
            set.spop(KEY);
        }, KEY);

        assertResult(result);
    }

    @Test
    void setBlockingWithWatchAndDiscard() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalSetCommands<String, String> set = tx.set(String.class);
            set.sadd(KEY, "a", "b");

            // Update the watched key from outside the transaction - that discards it.
            blockingDs.set(String.class).sadd(KEY, "outside");

            set.smembers(KEY);
            set.scard(KEY);
        }, KEY);

        assertThat(result.size()).isEqualTo(0);
        assertThat(result.discarded()).isTrue();
    }

    @Test
    void setReactive() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalSetCommands<String, String> set = tx.set(String.class);
            assertThat(set.getDataSource()).isEqualTo(tx);
            return set.sadd(KEY, "a", "b", "c", "d") // 0 -> 4
                    .chain(() -> set.scard(KEY)) // 1 -> 4
                    .chain(() -> set.smembers(KEY)) // 2 -> [a, b, c, d]
                    .chain(() -> set.sismember(KEY, "a")) // 3 -> true
                    .chain(() -> set.smismember(KEY, "a", "z")) // 4 -> [true, false]
                    .chain(() -> set.srandmember(KEY, 2)) // 5 -> two members
                    .chain(() -> set.srem(KEY, "a")) // 6 -> 1
                    .chain(() -> set.sunion(KEY, OTHER)) // 7 -> [b, c, d, x]
                    .chain(() -> set.spop(KEY)); // 8 -> one of b, c, d
        }).await().atMost(TIMEOUT);

        assertResult(result);
    }

    @Test
    void setReactiveWithWatch() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalSetCommands<String, String> set = tx.set(String.class);
            return set.sadd(KEY, "a", "b", "c", "d")
                    .chain(() -> set.scard(KEY))
                    .chain(() -> set.smembers(KEY))
                    .chain(() -> set.sismember(KEY, "a"))
                    .chain(() -> set.smismember(KEY, "a", "z"))
                    .chain(() -> set.srandmember(KEY, 2))
                    .chain(() -> set.srem(KEY, "a"))
                    .chain(() -> set.sunion(KEY, OTHER))
                    .chain(() -> set.spop(KEY));
        }, KEY).await().atMost(TIMEOUT);

        assertResult(result);
    }

    @Test
    void setReactiveWithWatchAndDiscard() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalSetCommands<String, String> set = tx.set(String.class);
            return set.sadd(KEY, "a", "b")
                    .chain(() -> reactiveDs.set(String.class).sadd(KEY, "outside"))
                    .chain(() -> set.smembers(KEY))
                    .chain(() -> set.scard(KEY));
        }, KEY).await().atMost(TIMEOUT);

        assertThat(result.size()).isEqualTo(0);
        assertThat(result.discarded()).isTrue();
    }

    /** Casts every entry to the exact type the non-transactional command returns — the point of the test. */
    @SuppressWarnings("unchecked")
    private static void assertResult(TransactionResult result) {
        assertThat(result.size()).isEqualTo(9);
        assertThat(result.discarded()).isFalse();
        assertThat((Integer) result.get(0)).isEqualTo(4);
        assertThat((Long) result.get(1)).isEqualTo(4L);
        assertThat((Set<String>) result.get(2)).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat((Boolean) result.get(3)).isTrue();
        assertThat((List<Boolean>) result.get(4)).containsExactly(true, false);
        assertThat((List<String>) result.get(5)).hasSize(2);
        assertThat((Integer) result.get(6)).isEqualTo(1);
        assertThat((Set<String>) result.get(7)).containsExactlyInAnyOrder("b", "c", "d", "x");
        assertThat((String) result.get(8)).isIn("b", "c", "d");
    }
}
