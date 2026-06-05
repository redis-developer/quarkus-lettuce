package io.quarkus.redis.runtime.client.lettuce.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.netty.channel.EventLoopGroup;
import io.quarkus.redis.datasource.keys.RedisValueType;
import io.quarkus.redis.datasource.transactions.OptimisticLockingTransactionResult;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.runtime.client.lettuce.LettuceClientResources;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;

@SuppressWarnings("resource")
class LettuceWithTransactionBlockingIntegrationTest {

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static Vertx vertx;
    static LettuceClientResources lettuceResources;
    static RedisClient redisClient;
    static StatefulRedisConnection<String, String> sharedConnection;
    static LettuceBlockingRedisDataSourceImpl ds;

    @BeforeAll
    static void setUp() {
        REDIS.start();
        vertx = Vertx.vertx();
        EventLoopGroup loops = ((VertxInternal) vertx.getDelegate()).getEventLoopGroup();
        lettuceResources = new LettuceClientResources(loops);
        String uri = String.format("redis://%s:%d", REDIS.getHost(), REDIS.getFirstMappedPort());
        redisClient = RedisClient.create(lettuceResources.clientResources(), uri);
        sharedConnection = redisClient.connect(StringCodec.UTF8);
        LettuceReactiveRedisDataSourceImpl reactive = new LettuceReactiveRedisDataSourceImpl(vertx, sharedConnection,
                () -> redisClient.connectAsync(StringCodec.UTF8, RedisURI.create(uri)));
        ds = new LettuceBlockingRedisDataSourceImpl(reactive, TIMEOUT);
    }

    @AfterAll
    static void tearDown() {
        if (sharedConnection != null) {
            sharedConnection.close();
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
        sharedConnection.sync().flushall();
    }

    private static long connectionCount() {
        String list = sharedConnection.sync().clientList();
        return list.isEmpty() ? 0 : list.split("\n").length;
    }

    @Test
    void execHappyPathReturnsOneEntryPerQueuedCommand() {
        TransactionResult result = ds.withTransaction(tx -> {
            var value = tx.value(String.class, String.class);
            value.set("k1", "v1");
            value.get("k1");
        });
        assertThat(result.discarded()).isFalse();
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.size()).isEqualTo(2);
        assertThat((String) result.get(1)).isEqualTo("v1");
        assertThat(sharedConnection.sync().get("k1")).isEqualTo("v1");
    }

    @Test
    void keyCommandsInTransactionYieldTypedResults() {
        sharedConnection.sync().set("k1", "v1");
        TransactionResult result = ds.withTransaction(tx -> {
            var keys = tx.key(String.class);
            keys.exists("k1");
            keys.expire("k1", 100);
            keys.ttl("k1");
            keys.type("k1");
            keys.rename("k1", "k2");
            keys.del("k2");
        });
        assertThat(result.discarded()).isFalse();
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.size()).isEqualTo(6);
        Boolean exists = result.get(0);
        Boolean expired = result.get(1);
        Long ttl = result.get(2);
        RedisValueType type = result.get(3);
        Object renamed = result.get(4);
        Integer deleted = result.get(5);
        assertThat(exists).isTrue();
        assertThat(expired).isTrue();
        assertThat(ttl).isGreaterThan(0L);
        assertThat(type).isEqualTo(RedisValueType.STRING);
        assertThat(renamed).isNull();
        assertThat(deleted).isEqualTo(1);
    }

    @Test
    void additionalMapperShapesYieldTypedResults() {
        sharedConnection.sync().set("k1", "v1");
        TransactionResult result = ds.withTransaction(tx -> {
            var value = tx.value(String.class, String.class);
            var keys = tx.key(String.class);
            value.setAndChanged("sc", "v");
            value.mget("k1", "missing");
            value.getrange("k1", 0, 1);
            keys.dump("k1");
        });
        assertThat(result.discarded()).isFalse();
        assertThat(result.hasErrors()).isFalse();
        assertThat(result.size()).isEqualTo(4);
        Boolean changed = result.get(0);
        Map<String, String> values = result.get(1);
        String range = result.get(2);
        String dump = result.get(3);
        assertThat(changed).isTrue();
        assertThat(values).hasSize(2);
        assertThat(values.get("k1")).isEqualTo("v1");
        assertThat(values.get("missing")).isNull();
        assertThat(range).isEqualTo("v1");
        assertThat(dump).isNotNull();
    }

    @Test
    void lcsInTransactionIsUnsupported() {
        assertThatThrownBy(() -> ds.withTransaction(tx -> tx.value(String.class, String.class).lcs("k1", "k2")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void userBlockExceptionIssuesDiscardAndPropagates() {
        long before = connectionCount();
        assertThatThrownBy(() -> ds.withTransaction(tx -> {
            tx.value(String.class, String.class).set("k", "v");
            throw new RuntimeException("boom");
        })).hasMessageContaining("boom");
        await().atMost(TIMEOUT).until(() -> connectionCount() == before);
        assertThat(sharedConnection.sync().get("k")).isNull();
    }

    @Test
    void explicitDiscardYieldsAbortedResult() {
        TransactionResult result = ds.withTransaction(tx -> {
            tx.value(String.class, String.class).set("k", "v");
            tx.discard();
        });
        assertThat(result.discarded()).isTrue();
        assertThat(sharedConnection.sync().get("k")).isNull();
    }

    @Test
    void watchViolationYieldsAbortedResult() {
        sharedConnection.sync().set("watched", "initial");
        TransactionResult result = ds.withTransaction(tx -> {
            sharedConnection.sync().set("watched", "changed");
            tx.value(String.class, String.class).set("k", "v");
        }, "watched");
        assertThat(result.discarded()).isTrue();
        assertThat(sharedConnection.sync().get("k")).isNull();
    }

    @Test
    void optimisticLockingPreTxRunsOnSameConnection() {
        sharedConnection.sync().set("counter", "10");
        OptimisticLockingTransactionResult<String> result = ds.withTransaction(
                preTx -> preTx.value(String.class, String.class).get("counter"),
                (current, tx) -> tx.value(String.class, String.class).set("counter", current + "0"),
                "counter");
        assertThat(result.discarded()).isFalse();
        assertThat(result.getPreTransactionResult()).isEqualTo("10");
        assertThat(sharedConnection.sync().get("counter")).isEqualTo("100");
    }

    @Test
    void rejectsNullAndEmptyWatchedKeys() {
        assertThatThrownBy(() -> ds.withTransaction(tx -> {
        }, (String[]) null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ds.withTransaction(tx -> {
        }, new String[0])).isInstanceOf(IllegalArgumentException.class);
    }
}
