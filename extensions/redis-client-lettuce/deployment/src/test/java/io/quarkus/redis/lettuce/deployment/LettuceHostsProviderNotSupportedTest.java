package io.quarkus.redis.lettuce.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.lettuce.core.RedisClient;
import io.quarkus.arc.InactiveBeanException;
import io.quarkus.redis.client.RedisClientName;
import io.quarkus.redis.client.RedisHostsProvider;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.annotation.Identifier;

/**
 * Tests that a Lettuce client configured only through {@code hosts-provider-name} is marked inactive with an
 * explanation, instead of failing at startup when the client factory is missing. The provider bean exists so that
 * the Vert.x client created for the same name (a working setup before adding the Lettuce extension) is unaffected.
 */
@SuppressWarnings("deprecation")
public class LettuceHostsProviderNotSupportedTest {

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClass(TestHostsProvider.class))
            .overrideConfigKey("quarkus.redis.devservices.enabled", "false")
            .overrideConfigKey("quarkus.redis.provided.hosts-provider-name", "my-provider")
            .assertException(e -> assertThat(e)
                    .satisfies(t -> assertThat(t.getClass().getName()).isEqualTo(InactiveBeanException.class.getName()))
                    .hasMessageContainingAll(
                            "Lettuce Redis Client 'provided' was deactivated automatically",
                            "hosts-provider-name",
                            "does not support"));

    @Inject
    @RedisClientName("provided")
    RedisClient redisClient;

    @Test
    void shouldNotRun() {
        // Build should fail before this runs
    }

    @ApplicationScoped
    @Identifier("my-provider")
    public static class TestHostsProvider implements RedisHostsProvider {

        @Override
        public Set<URI> getHosts() {
            return Set.of(URI.create("redis://localhost:6379"));
        }
    }
}
