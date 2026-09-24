package io.quarkus.redis.lettuce.runtime.internal.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.redis.lettuce.runtime.internal.CommandsTestBase;
import io.quarkus.redis.lettuce.runtime.internal.LettuceConnectionPool;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.redis.client.Response;

class LettuceWithConnectionReactiveIntegrationTest extends CommandsTestBase {

    LettuceReactiveRedisDataSourceImpl ds;

    @BeforeEach
    void initialize() {
        ds = reactiveDataSource();
    }

    @Test
    void runsBlockOnPinnedConnection() {
        AtomicLong captured = new AtomicLong();
        ds.withConnection(rds -> rds.execute("CLIENT", "ID")
                .map(Response::toLong)
                .invoke(captured::set)
                .replaceWithVoid()).await().atMost(TIMEOUT);
        long sharedId = connection.sync().clientId();
        assertThat(captured.get()).isPositive().isNotEqualTo(sharedId);
    }

    @Test
    void clientIdStableWithinBlock_reusedAcrossSequentialBlocks() {
        // Proves pooling, not connect-per-call: sequential withConnection calls borrow from a pool
        // of one idle connection, so the second block gets the exact connection the first released.
        AtomicLong first = new AtomicLong();
        AtomicLong second = new AtomicLong();
        ds.withConnection(rds -> rds.execute("CLIENT", "ID").map(Response::toLong)
                .invoke(first::set)
                .chain(() -> rds.execute("CLIENT", "ID").map(Response::toLong))
                .invoke(id -> assertThat(id).isEqualTo(first.get()))
                .replaceWithVoid()).await().atMost(TIMEOUT);
        ds.withConnection(rds -> rds.execute("CLIENT", "ID").map(Response::toLong)
                .invoke(second::set).replaceWithVoid()).await().atMost(TIMEOUT);
        assertThat(second.get()).isEqualTo(first.get());
    }

    @Test
    void nestedWithConnectionReusesOuterConnection() {
        AtomicLong outerId = new AtomicLong();
        AtomicLong innerId = new AtomicLong();
        ds.withConnection(outer -> outer.execute("CLIENT", "ID").map(Response::toLong)
                .invoke(outerId::set)
                .chain(() -> outer.withConnection(inner -> inner.execute("CLIENT", "ID").map(Response::toLong)
                        .invoke(innerId::set).replaceWithVoid())))
                .await().atMost(TIMEOUT);
        assertThat(innerId.get()).isEqualTo(outerId.get());
    }

    @Test
    void releasesConnectionToPoolOnFailure() {
        assertThatThrownBy(() -> ds.withConnection(rds -> Uni.createFrom().failure(new RuntimeException("boom")))
                .await().atMost(TIMEOUT)).hasMessageContaining("boom");
        await().atMost(TIMEOUT).until(() -> ds.getPool().getIdle() == ds.getPool().getObjectCount());
    }

    @Test
    void cancellationReleasesConnectionToPool() {
        AtomicReference<Cancellable> cancellable = new AtomicReference<>();
        AtomicLong capturedId = new AtomicLong(-1);
        cancellable.set(ds.withConnection(rds -> rds.execute("CLIENT", "ID").map(Response::toLong)
                .invoke(capturedId::set)
                .chain(() -> Uni.createFrom().nothing())).subscribe().with(x -> {
                }, t -> {
                }));
        await().atMost(TIMEOUT).until(() -> capturedId.get() != -1);
        cancellable.get().cancel();
        await().atMost(TIMEOUT).until(() -> ds.getPool().getIdle() == ds.getPool().getObjectCount());
    }

    @Test
    void thousandIterationsDoNotLeakConnections() {
        long before = connectionCount();
        for (int i = 0; i < 1000; i++) {
            ds.withConnection(rds -> rds.value(String.class, Integer.class).incr(key)
                    .replaceWithVoid()).await().atMost(TIMEOUT);
        }
        assertThat(ds.value(String.class, Integer.class).get(key).await().atMost(TIMEOUT)).isEqualTo(1000);
        await().atMost(TIMEOUT).until(() -> connectionCount() <= before + 1);
    }

    @Test
    void rejectsNullFunction() {
        assertThatThrownBy(() -> ds.withConnection(null).await().atMost(TIMEOUT))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void synchronousThrowInUserBlockIsCaughtAndConnectionReleased() {
        assertThatThrownBy(() -> ds.withConnection(rds -> {
            throw new RuntimeException("sync boom");
        }).await().atMost(TIMEOUT)).hasMessageContaining("sync boom");
        await().atMost(TIMEOUT).until(() -> ds.getPool().getIdle() == ds.getPool().getObjectCount());
    }

    @Test
    void connectorFailurePropagatesAndDoesNotLeak() {
        LettuceConnectionPool brokenPool = new LettuceConnectionPool(() -> {
            throw new RuntimeException("connector boom");
        }, MAX_POOL_SIZE, MAX_POOL_WAITING);
        LettuceReactiveRedisDataSourceImpl brokenDs = new LettuceReactiveRedisDataSourceImpl(
                vertx, connection, brokenPool);
        long before = connectionCount();
        assertThatThrownBy(() -> brokenDs.withConnection(rds -> Uni.createFrom().voidItem())
                .await().atMost(TIMEOUT)).hasMessageContaining("connector boom");
        assertThat(connectionCount()).isEqualTo(before);
    }

}
