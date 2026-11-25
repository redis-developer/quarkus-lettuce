package io.quarkus.redis.lettuce.deployment.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import jakarta.inject.Inject;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.builder.Version;
import io.quarkus.maven.dependency.Dependency;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.lettuce.deployment.RedisClusterTestResource;
import io.quarkus.redis.lettuce.deployment.RedisMeters;
import io.quarkus.redis.lettuce.deployment.RedisMeters.Commands;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.QuarkusTestResource;

/**
 * Verifies the Redis metrics of a cluster client of the Lettuce backend: a command of the application is recorded
 * once, whatever node it is routed to; the commands Lettuce sends to the nodes to discover and refresh the topology
 * are not.
 */
@QuarkusTestResource(RedisClusterTestResource.class)
public class LettuceClusterMetricsTest {

    private static final String DEFAULT = "<default>";

    @RegisterExtension
    static final QuarkusExtensionTest test = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(RedisMeters.class, Commands.class))
            .setForcedDependencies(List.of(
                    Dependency.of("io.quarkus", "quarkus-micrometer-deployment", Version.getVersion())))
            .overrideConfigKey("quarkus.redis.hosts", "${redis.cluster.hosts}")
            .overrideConfigKey("quarkus.redis.client-type", "cluster");

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

    /** The connection bean of the same client, used to look at the individual nodes. */
    @Inject
    StatefulRedisClusterConnection<String, String> cluster;

    @Test
    void recordsACommandOnceWhateverTheNode() {
        Commands before = RedisMeters.commands(registry, DEFAULT);

        Set<String> nodes = new HashSet<>();
        int commands = 0;
        for (int i = 0; i < 30; i++) {
            String key = "metrics-" + i;
            ds.value(String.class).set(key, "v");
            commands++;
            nodes.add(cluster.getPartitions().getPartitionBySlot(SlotHash.getSlot(key)).getNodeId());
        }

        assertThat(nodes).hasSizeGreaterThan(1);
        Commands expected = before.plus(commands, 0);
        RedisMeters.awaitCommands(registry, DEFAULT, expected);
        // and nothing more: what Lettuce sends to refresh the topology, every second here, is not recorded
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .until(() -> RedisMeters.commands(registry, DEFAULT).equals(expected));
    }
}
