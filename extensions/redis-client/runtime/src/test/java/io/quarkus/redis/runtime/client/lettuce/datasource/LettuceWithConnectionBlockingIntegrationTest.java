package io.quarkus.redis.runtime.client.lettuce.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.runtime.client.lettuce.CommandsTestBase;

class LettuceWithConnectionBlockingIntegrationTest extends CommandsTestBase {

    RedisDataSource ds;

    @BeforeEach
    void initialize() {
        ds = blockingDataSource();
    }

    @Test
    void runsBlockOnPinnedConnection() {
        AtomicLong captured = new AtomicLong();
        ds.withConnection(rds -> captured.set(rds.execute("CLIENT", "ID").toLong()));
        long sharedId = connection.sync().clientId();
        assertThat(captured.get()).isPositive().isNotEqualTo(sharedId);
    }

    @Test
    void clientIdStableWithinBlock_differsAcrossBlocks() {
        AtomicLong first = new AtomicLong();
        AtomicLong second = new AtomicLong();
        ds.withConnection(rds -> {
            long a = rds.execute("CLIENT", "ID").toLong();
            long b = rds.execute("CLIENT", "ID").toLong();
            assertThat(a).isEqualTo(b);
            first.set(a);
        });
        ds.withConnection(rds -> second.set(rds.execute("CLIENT", "ID").toLong()));
        assertThat(first.get()).isNotEqualTo(second.get());
    }

    @Test
    void nestedWithConnectionReusesOuterConnection() {
        AtomicLong outerId = new AtomicLong();
        AtomicLong innerId = new AtomicLong();
        ds.withConnection(outer -> {
            outerId.set(outer.execute("CLIENT", "ID").toLong());
            outer.withConnection(inner -> innerId.set(inner.execute("CLIENT", "ID").toLong()));
        });
        assertThat(innerId.get()).isEqualTo(outerId.get());
    }

    @Test
    void releasesConnectionOnFailure() {
        long before = connectionCount();
        assertThatThrownBy(() -> ds.withConnection(rds -> {
            throw new RuntimeException("boom");
        })).hasMessageContaining("boom");
        await().atMost(TIMEOUT).until(() -> connectionCount() == before);
    }

    @Test
    void thousandIterationsDoNotLeakConnections() {
        long before = connectionCount();
        for (int i = 0; i < 1000; i++) {
            ds.withConnection(rds -> rds.value(String.class, Integer.class).incr(key));
        }
        assertThat(ds.value(String.class, Integer.class).get(key)).isEqualTo(1000);
        await().atMost(TIMEOUT).until(() -> connectionCount() <= before + 1);
    }

    @Test
    void rejectsNullConsumer() {
        assertThatThrownBy(() -> ds.withConnection(null)).isInstanceOf(NullPointerException.class);
    }

}
