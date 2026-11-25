package io.quarkus.redis.lettuce.deployment.sentinel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;

import jakarta.inject.Inject;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.builder.Version;
import io.quarkus.maven.dependency.Dependency;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.lettuce.deployment.RedisMeters;
import io.quarkus.redis.lettuce.deployment.RedisMeters.Commands;
import io.quarkus.redis.lettuce.deployment.RedisSentinelTestResource;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.QuarkusTestResource;

/**
 * Verifies the Redis metrics of a Sentinel client of the Lettuce backend: the commands of the application are
 * recorded once each; the ones Lettuce sends to the sentinels and to the nodes to discover the master and its
 * replicas, and to follow a failover, are not.
 */
@QuarkusTestResource(RedisSentinelTestResource.class)
public class LettuceSentinelMetricsTest {

    private static final String DEFAULT = "<default>";

    @RegisterExtension
    static final QuarkusExtensionTest test = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(RedisMeters.class, Commands.class))
            .setForcedDependencies(List.of(
                    Dependency.of("io.quarkus", "quarkus-micrometer-deployment", Version.getVersion())))
            .overrideConfigKey("quarkus.redis.hosts", "${redis.sentinel.hosts}")
            .overrideConfigKey("quarkus.redis.client-type", "sentinel")
            .overrideConfigKey("quarkus.redis.master-name", "${redis.sentinel.master-name}")
            .overrideConfigKey("quarkus.redis.replicas", "share");

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

    @Test
    void recordsOnlyTheCommandsOfTheApplication() {
        Commands before = RedisMeters.commands(registry, DEFAULT);

        // writes go to the master, reads to the nodes the configuration selects
        for (int i = 0; i < 10; i++) {
            ds.value(String.class).set("metrics-" + i, "v");
            assertThat(ds.value(String.class).get("metrics-" + i)).isIn("v", null);
        }
        // a pooled connection, opened for the blocking command
        assertThat(ds.list(String.class).blpop(Duration.ofMillis(100), "metrics-empty")).isNull();

        Commands expected = before.plus(21, 0);
        RedisMeters.awaitCommands(registry, DEFAULT, expected);
        // and nothing more: what Lettuce sends to follow the topology is not recorded
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .until(() -> RedisMeters.commands(registry, DEFAULT).equals(expected));
    }
}
