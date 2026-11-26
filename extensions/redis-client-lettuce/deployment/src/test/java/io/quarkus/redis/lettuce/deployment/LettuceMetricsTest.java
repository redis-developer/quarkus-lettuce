package io.quarkus.redis.lettuce.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.builder.Version;
import io.quarkus.maven.dependency.Dependency;
import io.quarkus.redis.client.RedisClientName;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.lettuce.deployment.RedisMeters.Commands;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.QuarkusTestResource;

/**
 * Verifies that, with {@code quarkus-micrometer}, the commands sent through the Lettuce backend are recorded by the
 * {@code redis.commands.*} meters the Vert.x client records, per client, and that the pool of a client is observed
 * by the {@code redis.pool.*} meters the pool of the Vert.x client is observed by.
 */
@QuarkusTestResource(RedisTestResource.class)
public class LettuceMetricsTest {

    private static final String DEFAULT = "<default>";

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(RedisMeters.class, Commands.class))
            .setForcedDependencies(List.of(
                    Dependency.of("io.quarkus", "quarkus-micrometer-deployment", Version.getVersion())))
            .overrideConfigKey("quarkus.redis.devservices.enabled", "false")
            .overrideConfigKey("quarkus.redis.hosts", "${quarkus.redis.tr}")
            .overrideConfigKey("quarkus.redis.named.hosts", "${quarkus.redis.tr}")
            .overrideConfigKey("quarkus.redis.impatient.hosts", "${quarkus.redis.tr}")
            .overrideConfigKey("quarkus.redis.impatient.timeout", "200ms");

    static final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @BeforeAll
    static void setRegistry() {
        Metrics.addRegistry(registry);
    }

    @AfterAll
    static void removeRegistry() {
        Metrics.removeRegistry(registry);
    }

    @Inject
    RedisDataSource ds;

    @Inject
    @RedisClientName("named")
    RedisDataSource named;

    /** Reactive: the blocking data source gives up waiting when the timeout elapses, whatever became of the command. */
    @Inject
    @RedisClientName("impatient")
    ReactiveRedisDataSource impatient;

    @Test
    void recordsTheCommands() {
        Commands before = RedisMeters.commands(registry, DEFAULT);

        String key = key();
        ds.value(String.class).set(key, "1");
        ds.value(Integer.class).incr(key);
        assertThat(ds.value(String.class).get(key)).isEqualTo("2");

        RedisMeters.awaitCommands(registry, DEFAULT, before.plus(3, 0));
        assertThat(registry.get("redis.commands.duration").tag("client-name", DEFAULT).timer().mean(TimeUnit.NANOSECONDS))
                .isPositive();
    }

    @Test
    void recordsACommandTheServerRejects() {
        String key = key();
        Commands before = RedisMeters.commands(registry, DEFAULT);

        ds.value(String.class).set(key, "not a number");
        assertThatThrownBy(() -> ds.value(String.class).incr(key)).hasMessageContaining("not an integer");

        RedisMeters.awaitCommands(registry, DEFAULT, before.plus(2, 1));
    }

    @Test
    void recordsACommandThatTimesOut() {
        Commands before = RedisMeters.commands(registry, "impatient");
        Commands defaultBefore = RedisMeters.commands(registry, DEFAULT);

        // connects, so that the handshake is not what times out
        impatient.execute("PING").await().atMost(Duration.ofSeconds(5));
        // the server stops answering for longer than the timeout of the client
        ds.execute("CLIENT", "PAUSE", "1000", "ALL");
        try {
            assertThatThrownBy(() -> impatient.value(String.class).get(key()).await().atMost(Duration.ofSeconds(5)))
                    .hasMessageContaining("timed out");
        } finally {
            // answered when the pause ends: the next test finds the server answering
            ds.execute("PING");
        }

        RedisMeters.awaitCommands(registry, "impatient", before.plus(2, 1));
        RedisMeters.awaitCommands(registry, DEFAULT, defaultBefore.plus(2, 0));
    }

    @Test
    void tagsTheClientsApart() {
        Commands defaultBefore = RedisMeters.commands(registry, DEFAULT);
        Commands namedBefore = RedisMeters.commands(registry, "named");

        named.value(String.class).set(key(), "a");
        named.value(String.class).set(key(), "b");
        ds.value(String.class).set(key(), "c");

        RedisMeters.awaitCommands(registry, "named", namedBefore.plus(2, 0));
        RedisMeters.awaitCommands(registry, DEFAULT, defaultBefore.plus(1, 0));
    }

    @Test
    void recordsEveryCommandOfATransaction() {
        Commands before = RedisMeters.commands(registry, DEFAULT);

        String key = key();
        ds.withTransaction(tx -> {
            tx.value(String.class).set(key, "a");
            tx.value(String.class).append(key, "b");
        });

        // MULTI, the two queued commands and EXEC
        RedisMeters.awaitCommands(registry, DEFAULT, before.plus(4, 0));
    }

    @Test
    void recordsABlockingCommandOnAPooledConnection() {
        Commands before = RedisMeters.commands(registry, DEFAULT);

        assertThat(ds.list(String.class).blpop(Duration.ofMillis(100), key())).isNull();

        RedisMeters.awaitCommands(registry, DEFAULT, before.plus(1, 0));
    }

    @Test
    void observesThePoolAsThePoolOfTheVertxClientIsObserved() {
        Commands before = RedisMeters.commands(registry, DEFAULT);
        long completedBefore = RedisMeters.poolCompleted(registry, DEFAULT);

        ds.withConnection(pinned -> {
            pinned.execute("PING");
            assertThat(RedisMeters.pool(registry, "active", DEFAULT)).isEqualTo(1);
            // of the six connections the pool may open
            assertThat(RedisMeters.pool(registry, "idle", DEFAULT)).isEqualTo(5);
            assertThat(RedisMeters.pool(registry, "ratio", DEFAULT)).isEqualTo(1.0 / 6);
            assertThat(RedisMeters.pool(registry, "queue.size", DEFAULT)).isZero();
        });

        assertThat(RedisMeters.pool(registry, "active", DEFAULT)).isZero();
        assertThat(RedisMeters.pool(registry, "idle", DEFAULT)).isEqualTo(6);
        assertThat(RedisMeters.poolCompleted(registry, DEFAULT)).isEqualTo(completedBefore + 1);
        assertThat(registry.get("redis.pool.usage").tag("pool.name", DEFAULT).timer().count()).isPositive();
        // the pool of another client is observed apart, and has not been used
        assertThat(RedisMeters.pool(registry, "active", "named")).isZero();
        assertThat(RedisMeters.poolCompleted(registry, "named")).isZero();
        RedisMeters.awaitCommands(registry, DEFAULT, before.plus(1, 0));
    }

    private static String key() {
        return "metrics-" + UUID.randomUUID();
    }
}
