package io.quarkus.redis.runtime.client.lettuce.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.netty.channel.EventLoopGroup;
import io.quarkus.redis.runtime.client.lettuce.LettuceClientResources;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Vertx;

@SuppressWarnings("resource")
class LettuceWithConnectionBlockingIntegrationTest {

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
        EventLoopGroup loops = ((VertxInternal) vertx.getDelegate()).eventLoopGroup();
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

    private static long connectionCount() {
        String list = sharedConnection.sync().clientList();
        return list.isEmpty() ? 0 : list.split("\n").length;
    }

    @Test
    void runsBlockOnPinnedConnection() {
        AtomicLong captured = new AtomicLong();
        ds.withConnection(rds -> captured.set(rds.execute("CLIENT", "ID").toLong()));
        long sharedId = sharedConnection.sync().clientId();
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
            ds.withConnection(rds -> rds.execute("CLIENT", "ID"));
        }
        await().atMost(TIMEOUT).until(() -> connectionCount() <= before + 1);
    }

    @Test
    void rejectsNullConsumer() {
        assertThatThrownBy(() -> ds.withConnection(null)).isInstanceOf(NullPointerException.class);
    }
}
