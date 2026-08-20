package io.quarkus.redis.runtime.client.lettuce.list;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.list.KeyValue;
import io.quarkus.redis.datasource.list.Position;
import io.quarkus.redis.datasource.list.ReactiveTransactionalListCommands;
import io.quarkus.redis.datasource.list.TransactionalListCommands;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.runtime.client.lettuce.CommandsTestBase;

class LettuceTransactionalListCommandsTest extends CommandsTestBase {

    private static final String KEY = "tx-list-key";

    private RedisDataSource blockingDs;
    private ReactiveRedisDataSource reactiveDs;

    @BeforeEach
    void initialize() {
        blockingDs = blockingDataSource(Duration.ofSeconds(60));
        reactiveDs = reactiveDataSource();
    }

    @Test
    void listBlocking() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalListCommands<String, String> list = tx.list(String.class);
            assertThat(list.getDataSource()).isEqualTo(tx);
            list.lpop(KEY); // 0 -> null
            list.rpush(KEY, "v1", "v2"); // 1 -> 2
            list.lrange(KEY, 0, -1); // 2 -> [v1, v2]
            list.lpos(KEY, "v2"); // 3 -> 1
            list.lmpop(Position.LEFT, KEY); // 4 -> (KEY, v1)
            list.ltrim(KEY, 0, -1); // 5 -> null
            list.llen(KEY); // 6 -> 1
        });

        assertResult(result);
    }

    @Test
    void listBlockingWithWatch() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalListCommands<String, String> list = tx.list(String.class);
            list.lpop(KEY);
            list.rpush(KEY, "v1", "v2");
            list.lrange(KEY, 0, -1);
            list.lpos(KEY, "v2");
            list.lmpop(Position.LEFT, KEY);
            list.ltrim(KEY, 0, -1);
            list.llen(KEY);
        }, KEY);

        assertResult(result);
    }

    @Test
    void listBlockingWithWatchAndDiscard() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalListCommands<String, String> list = tx.list(String.class);
            list.lpop(KEY);
            list.rpush(KEY, "v1", "v2");

            // Update the watched key from outside the transaction - that discards it.
            blockingDs.list(String.class).rpush(KEY, "outside");

            list.lrange(KEY, 0, -1);
            list.llen(KEY);
        }, KEY);

        assertThat(result.size()).isEqualTo(0);
        assertThat(result.discarded()).isTrue();
    }

    @Test
    void listReactive() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalListCommands<String, String> list = tx.list(String.class);
            assertThat(list.getDataSource()).isEqualTo(tx);
            return list.lpop(KEY) // 0 -> null
                    .chain(() -> list.rpush(KEY, "v1", "v2")) // 1 -> 2
                    .chain(() -> list.lrange(KEY, 0, -1)) // 2 -> [v1, v2]
                    .chain(() -> list.lpos(KEY, "v2")) // 3 -> 1
                    .chain(() -> list.lmpop(Position.LEFT, KEY)) // 4 -> (KEY, v1)
                    .chain(() -> list.ltrim(KEY, 0, -1)) // 5 -> null
                    .chain(() -> list.llen(KEY)); // 6 -> 1
        }).await().atMost(TIMEOUT);

        assertResult(result);
    }

    @Test
    void listReactiveWithWatch() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalListCommands<String, String> list = tx.list(String.class);
            return list.lpop(KEY)
                    .chain(() -> list.rpush(KEY, "v1", "v2"))
                    .chain(() -> list.lrange(KEY, 0, -1))
                    .chain(() -> list.lpos(KEY, "v2"))
                    .chain(() -> list.lmpop(Position.LEFT, KEY))
                    .chain(() -> list.ltrim(KEY, 0, -1))
                    .chain(() -> list.llen(KEY));
        }, KEY).await().atMost(TIMEOUT);

        assertResult(result);
    }

    @Test
    void listReactiveWithWatchAndDiscard() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalListCommands<String, String> list = tx.list(String.class);
            return list.lpop(KEY)
                    .chain(() -> list.rpush(KEY, "v1", "v2"))
                    .chain(() -> reactiveDs.list(String.class).rpush(KEY, "outside"))
                    .chain(() -> list.lrange(KEY, 0, -1))
                    .chain(() -> list.llen(KEY));
        }, KEY).await().atMost(TIMEOUT);

        assertThat(result.size()).isEqualTo(0);
        assertThat(result.discarded()).isTrue();
    }

    /** Casts every entry to the exact type the non-transactional command returns — the point of the test. */
    @SuppressWarnings("unchecked")
    private static void assertResult(TransactionResult result) {
        assertThat(result.size()).isEqualTo(7);
        assertThat(result.discarded()).isFalse();
        assertThat((String) result.get(0)).isNull();
        assertThat((Long) result.get(1)).isEqualTo(2L);
        assertThat((List<String>) result.get(2)).containsExactly("v1", "v2");
        assertThat((Long) result.get(3)).isEqualTo(1L);
        assertThat((KeyValue<String, String>) result.get(4)).isEqualTo(KeyValue.of(KEY, "v1"));
        assertThat((Void) result.get(5)).isNull();
        assertThat((Long) result.get(6)).isEqualTo(1L);
    }
}
