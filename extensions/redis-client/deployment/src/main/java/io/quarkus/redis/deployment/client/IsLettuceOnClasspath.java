package io.quarkus.redis.deployment.client;

import java.util.function.BooleanSupplier;

import io.quarkus.bootstrap.classloading.QuarkusClassLoader;

/**
 * Build-step predicate that passes when {@code lettuce-core} is present on the application's
 * runtime classpath.
 * <p>
 * Lettuce is an optional dependency of the Redis extension, so the Lettuce-specific build steps
 * have nothing to produce when it is absent and are skipped entirely. Selecting the Lettuce
 * backend without the dependency is reported separately by the backend resolution step.
 */
public class IsLettuceOnClasspath implements BooleanSupplier {

    static final String LETTUCE_MARKER_CLASS = "io.lettuce.core.RedisClient";

    @Override
    public boolean getAsBoolean() {
        return QuarkusClassLoader.isClassPresentAtRuntime(LETTUCE_MARKER_CLASS);
    }
}
