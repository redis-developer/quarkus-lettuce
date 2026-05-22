package io.quarkus.redis.runtime.client.lettuce.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.netty.channel.EventLoopGroup;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.runtime.client.lettuce.LettuceClientResources;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.redis.client.Response;

@SuppressWarnings("resource")
class LettuceWithConnectionReactiveIntegrationTest {

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static Vertx vertx;
    static LettuceClientResources lettuceResources;
    static RedisClient redisClient;
    static StatefulRedisConnection<String, String> sharedConnection;
    static LettuceReactiveRedisDataSourceImpl ds;

    @BeforeAll
    static void setUp() {
        REDIS.start();
        vertx = Vertx.vertx();
        EventLoopGroup loops = ((VertxInternal) vertx.getDelegate()).getEventLoopGroup();
        lettuceResources = new LettuceClientResources(loops);
        String uri = String.format("redis://%s:%d", REDIS.getHost(), REDIS.getFirstMappedPort());
        redisClient = RedisClient.create(lettuceResources.clientResources(), uri);
        sharedConnection = redisClient.connect(StringCodec.UTF8);
        ds = new LettuceReactiveRedisDataSourceImpl(vertx, sharedConnection,
                () -> redisClient.connect(StringCodec.UTF8));
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

    private static long clientId(ReactiveRedisDataSource rds) {
        return rds.execute("CLIENT", "ID").map(Response::toLong).await().atMost(TIMEOUT);
    }

    private static long connectionCount() {
        String list = sharedConnection.sync().clientList();
        return list.isEmpty() ? 0 : list.split("\n").length;
    }

    @Test
    void runsBlockOnPinnedConnection() {
        AtomicLong captured = new AtomicLong();
        ds.withConnection(rds -> rds.execute("CLIENT", "ID")
                .map(Response::toLong)
                .invoke(captured::set)
                .replaceWithVoid()).await().atMost(TIMEOUT);
        long sharedId = sharedConnection.sync().clientId();
        assertThat(captured.get()).isPositive().isNotEqualTo(sharedId);
    }

    @Test
    void clientIdStableWithinBlock_differsAcrossBlocks() {
        AtomicLong first = new AtomicLong();
        AtomicLong second = new AtomicLong();
        ds.withConnection(rds -> rds.execute("CLIENT", "ID").map(Response::toLong)
                .invoke(first::set)
                .chain(() -> rds.execute("CLIENT", "ID").map(Response::toLong))
                .invoke(id -> assertThat(id).isEqualTo(first.get()))
                .replaceWithVoid()).await().atMost(TIMEOUT);
        ds.withConnection(rds -> rds.execute("CLIENT", "ID").map(Response::toLong)
                .invoke(second::set).replaceWithVoid()).await().atMost(TIMEOUT);
        assertThat(first.get()).isNotEqualTo(second.get());
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
    void releasesConnectionOnFailure() {
        long before = connectionCount();
        assertThatThrownBy(() -> ds.withConnection(rds -> Uni.createFrom().<Void> failure(new RuntimeException("boom")))
                .await().atMost(TIMEOUT)).hasMessageContaining("boom");
        await().atMost(TIMEOUT).until(() -> connectionCount() == before);
    }

    @Test
    void cancellationClosesConnection() {
        long before = connectionCount();
        AtomicReference<Cancellable> cancellable = new AtomicReference<>();
        AtomicLong capturedId = new AtomicLong(-1);
        cancellable.set(ds.withConnection(rds -> rds.execute("CLIENT", "ID").map(Response::toLong)
                .invoke(capturedId::set)
                .chain(() -> Uni.createFrom().<Void> nothing())).subscribe().with(x -> {
                }, t -> {
                }));
        await().atMost(TIMEOUT).until(() -> capturedId.get() != -1);
        cancellable.get().cancel();
        await().atMost(TIMEOUT).until(() -> connectionCount() == before);
    }

    @Test
    void thousandIterationsDoNotLeakConnections() {
        long before = connectionCount();
        for (int i = 0; i < 1000; i++) {
            ds.withConnection(rds -> rds.execute("CLIENT", "ID").replaceWithVoid()).await().atMost(TIMEOUT);
        }
        await().atMost(TIMEOUT).until(() -> connectionCount() <= before + 1);
    }

    @Test
    void rejectsNullFunction() {
        assertThatThrownBy(() -> ds.withConnection(null).await().atMost(TIMEOUT))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void synchronousThrowInUserBlockIsCaughtAndConnectionReleased() {
        long before = connectionCount();
        assertThatThrownBy(() -> ds.withConnection(rds -> {
            throw new RuntimeException("sync boom");
        }).await().atMost(TIMEOUT)).hasMessageContaining("sync boom");
        await().atMost(TIMEOUT).until(() -> connectionCount() == before);
    }

    @Test
    void connectorFailurePropagatesAndDoesNotLeak() {
        LettuceReactiveRedisDataSourceImpl brokenDs = new LettuceReactiveRedisDataSourceImpl(
                vertx, sharedConnection, () -> {
                    throw new RuntimeException("connector boom");
                });
        long before = connectionCount();
        assertThatThrownBy(() -> brokenDs.withConnection(rds -> Uni.createFrom().<Void> voidItem())
                .await().atMost(TIMEOUT)).hasMessageContaining("connector boom");
        assertThat(connectionCount()).isEqualTo(before);
    }
}
