package io.quarkus.redis.lettuce.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.inject.Instance;
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
import io.quarkus.redis.lettuce.deployment.RedisMeters.Commands;
import io.quarkus.redis.runtime.client.ObservableRedisMetrics;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.QuarkusTestResource;

/**
 * Verifies that, with {@code quarkus.micrometer.binder.redis.enabled=false}, the Lettuce backend records no command:
 * there is no metrics bean, so the commands are not observed, as with the Vert.x client. (The pool metrics are
 * those of Vert.x, and follow {@code quarkus.micrometer.binder.vertx.enabled}, as for the Vert.x client.)
 */
@QuarkusTestResource(RedisTestResource.class)
public class LettuceMetricsDisabledTest {

    private static final String DEFAULT = "<default>";

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(RedisMeters.class, Commands.class))
            .setForcedDependencies(List.of(
                    Dependency.of("io.quarkus", "quarkus-micrometer-deployment", Version.getVersion())))
            .overrideConfigKey("quarkus.micrometer.binder.redis.enabled", "false")
            .overrideConfigKey("quarkus.redis.devservices.enabled", "false")
            .overrideConfigKey("quarkus.redis.hosts", "${quarkus.redis.tr}");

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
    Instance<ObservableRedisMetrics> metrics;

    @Test
    void recordsNothing() {
        assertThat(metrics.isUnsatisfied()).isTrue();

        ds.value(String.class).set("metrics-" + UUID.randomUUID(), "a");
        ds.withConnection(pinned -> pinned.execute("PING"));

        // meters left in the global registry by another test of this JVM read as zero in this registry; a command
        // is recorded a moment after it completes, so look for a while
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                .until(() -> RedisMeters.commands(registry, DEFAULT).equals(new Commands(0, 0, 0, 0)));
    }
}
