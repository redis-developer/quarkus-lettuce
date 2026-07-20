package io.quarkus.redis.runtime.client.lettuce;

import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;

import io.lettuce.core.resource.ClientResources;
import io.netty.channel.EventLoopGroup;

/**
 * Provides Lettuce {@link ClientResources} configured to reuse Vert.x-managed Netty event loops
 * via a {@link VertxEventLoopGroupProvider}.
 * <p>
 * Lettuce will not create its own event loop threads. Timer, computation executor, and DNS resolver
 * remain Lettuce-owned and are shut down when {@link #shutdown()} is called.
 */
public class LettuceClientResources {

    private static final Logger LOGGER = Logger.getLogger(LettuceClientResources.class);

    private final ClientResources clientResources;

    /**
     * Creates Lettuce {@link ClientResources} that share the given Vert.x-managed event loop group.
     *
     * @param vertxEventLoopGroup the Vert.x-managed Netty event loop group
     */
    public LettuceClientResources(EventLoopGroup vertxEventLoopGroup) {
        LOGGER.info("Creating Lettuce ClientResources with shared Vert.x event loops");

        VertxEventLoopGroupProvider eventLoopGroupProvider = new VertxEventLoopGroupProvider(vertxEventLoopGroup);

        this.clientResources = ClientResources.builder()
                .eventLoopGroupProvider(eventLoopGroupProvider)
                .build();
    }

    /**
     * Returns the configured {@link ClientResources} instance.
     */
    public ClientResources clientResources() {
        return clientResources;
    }

    /**
     * Shuts down Lettuce-owned resources (timer, computation executor, DNS resolver).
     * Does <b>not</b> shut down the Vert.x-managed event loop group.
     *
     * @param quietPeriodMs quiet period in milliseconds before forceful shutdown
     * @param timeoutMs maximum time to wait for shutdown in milliseconds
     */
    public void shutdown(long quietPeriodMs, long timeoutMs) {
        LOGGER.info("Shutting down Lettuce ClientResources (event loops remain Vert.x-managed)");
        clientResources.shutdown(quietPeriodMs, timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Shuts down with default quiet period (100ms) and timeout (2000ms).
     */
    public void shutdown() {
        shutdown(100, 2000);
    }
}
