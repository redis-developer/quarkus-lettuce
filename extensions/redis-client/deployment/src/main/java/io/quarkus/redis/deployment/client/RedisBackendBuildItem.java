package io.quarkus.redis.deployment.client;

import io.quarkus.builder.item.SimpleBuildItem;

/**
 * Carries the Redis backend that should be used to produce the
 * {@link io.quarkus.redis.datasource.RedisDataSource} and
 * {@link io.quarkus.redis.datasource.ReactiveRedisDataSource} synthetic beans.
 * <p>
 * Resolved once at build time from a combination of classpath detection and the
 * {@code quarkus.redis.backend} configuration property.
 */
public final class RedisBackendBuildItem extends SimpleBuildItem {

    public enum Backend {
        VERTX,
        LETTUCE
    }

    private final Backend backend;

    public RedisBackendBuildItem(Backend backend) {
        this.backend = backend;
    }

    public Backend backend() {
        return backend;
    }

    public boolean isLettuce() {
        return backend == Backend.LETTUCE;
    }

    public boolean isVertx() {
        return backend == Backend.VERTX;
    }
}
