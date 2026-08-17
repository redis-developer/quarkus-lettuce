package io.quarkus.redis.runtime.client.lettuce.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.hash.ReactiveTransactionalHashCommands;
import io.quarkus.redis.datasource.hash.TransactionalHashCommands;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.runtime.client.lettuce.CommandsTestBase;

class LettuceTransactionalHashCommandsTest extends CommandsTestBase {

    private static final String KEY = "tx-hash-key";

    private RedisDataSource blockingDs;
    private ReactiveRedisDataSource reactiveDs;

    @BeforeEach
    void initialize() {
        blockingDs = blockingDataSource(Duration.ofSeconds(60));
        reactiveDs = reactiveDataSource();
    }

    @Test
    void hgetBlocking() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalHashCommands<String, String, String> hash = tx.hash(String.class);
            assertThat(hash.getDataSource()).isEqualTo(tx);
            hash.hget(KEY, "field"); // 0 -> null
            hash.hset(KEY, "field", "hello"); // 1 -> true
            hash.hget(KEY, "field"); // 2 -> "hello"
            hash.hdel(KEY, "field", "field2"); // 3 -> 1
            hash.hget(KEY, "field"); // 4 -> null
        });

        assertThat(result.size()).isEqualTo(5);
        assertThat(result.discarded()).isFalse();
        assertThat((Void) result.get(0)).isNull();
        assertThat((Boolean) result.get(1)).isTrue();
        assertThat((String) result.get(2)).isEqualTo("hello");
        assertThat((int) result.get(3)).isEqualTo(1);
        assertThat((Void) result.get(4)).isNull();
    }

    @Test
    void hgetBlockingWithWatch() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalHashCommands<String, String, String> hash = tx.hash(String.class);
            hash.hget(KEY, "field"); // 0 -> null
            hash.hset(KEY, "field", "hello"); // 1 -> true
            hash.hget(KEY, "field"); // 2 -> "hello"
            hash.hdel(KEY, "field", "field2"); // 3 -> 1
            hash.hget(KEY, "field"); // 4 -> null
        }, KEY);
        assertThat(result.size()).isEqualTo(5);
        assertThat(result.discarded()).isFalse();
        assertThat((Void) result.get(0)).isNull();
        assertThat((Boolean) result.get(1)).isTrue();
        assertThat((String) result.get(2)).isEqualTo("hello");
        assertThat((int) result.get(3)).isEqualTo(1);
        assertThat((Void) result.get(4)).isNull();
    }

    @Test
    void hgetBlockingWithWatchAndDiscard() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalHashCommands<String, String, String> hash = tx.hash(String.class);
            hash.hget(KEY, "field");
            hash.hset(KEY, "field", "hello");
            hash.hget(KEY, "field");

            // Update the watched key from outside the transaction - that discards it.
            blockingDs.hash(String.class).hset(KEY, "toto", "updated");

            hash.hdel(KEY, "field", "field2");
            hash.hget(KEY, "field");
        }, KEY);

        assertThat(result.size()).isEqualTo(0);
        assertThat(result.discarded()).isTrue();
    }

    @Test
    void hgetReactive() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalHashCommands<String, String, String> hash = tx.hash(String.class);
            return hash.hget(KEY, "field") // 0 -> null
                    .chain(() -> hash.hset(KEY, "field", "hello")) // 1 -> true
                    .chain(() -> hash.hget(KEY, "field")) // 2 -> "hello"
                    .chain(() -> hash.hdel(KEY, "field", "field2")) // 3 -> 1
                    .chain(() -> hash.hget(KEY, "field")); // 4 -> null
        }).await().atMost(TIMEOUT);
        assertThat(result.size()).isEqualTo(5);
        assertThat(result.discarded()).isFalse();
        assertThat((Void) result.get(0)).isNull();
        assertThat((Boolean) result.get(1)).isTrue();
        assertThat((String) result.get(2)).isEqualTo("hello");
        assertThat((int) result.get(3)).isEqualTo(1);
        assertThat((Void) result.get(4)).isNull();
    }

    @Test
    void hgetReactiveWithWatch() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalHashCommands<String, String, String> hash = tx.hash(String.class);
            return hash.hget(KEY, "field") // 0 -> null
                    .chain(() -> hash.hset(KEY, "field", "hello")) // 1 -> true
                    .chain(() -> hash.hget(KEY, "field")) // 2 -> "hello"
                    .chain(() -> hash.hdel(KEY, "field", "field2")) // 3 -> 1
                    .chain(() -> hash.hget(KEY, "field")); // 4 -> null
        }, KEY).await().atMost(TIMEOUT);
        assertThat(result.size()).isEqualTo(5);
        assertThat(result.discarded()).isFalse();
        assertThat((Void) result.get(0)).isNull();
        assertThat((Boolean) result.get(1)).isTrue();
        assertThat((String) result.get(2)).isEqualTo("hello");
        assertThat((int) result.get(3)).isEqualTo(1);
        assertThat((Void) result.get(4)).isNull();
    }

    @Test
    void hgetReactiveWithWatchAndDiscard() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalHashCommands<String, String, String> hash = tx.hash(String.class);
            return hash.hget(KEY, "field")
                    .chain(() -> hash.hset(KEY, "field", "hello"))
                    .chain(() -> hash.hget(KEY, "field"))
                    .chain(() -> reactiveDs.hash(String.class).hset(KEY, "a", "b"))
                    .chain(() -> hash.hdel(KEY, "field", "field2"))
                    .chain(() -> hash.hget(KEY, "field"));
        }, KEY).await().atMost(TIMEOUT);
        assertThat(result.size()).isEqualTo(0);
        assertThat(result.discarded()).isTrue();
    }

}
