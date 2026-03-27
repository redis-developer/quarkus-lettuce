package io.quarkus.redis.runtime.client.lettuce;

import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;

import io.lettuce.core.resource.EventLoopGroupProvider;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.SucceededFuture;

/**
 * An {@link EventLoopGroupProvider} that delegates to a Vert.x-managed {@link EventLoopGroup}.
 * <p>
 * This provider does <b>not</b> own the event loop group and will never shut it down.
 * Shutdown is Vert.x's responsibility. The {@link #release} and {@link #shutdown} methods
 * are no-ops that return immediately-succeeded futures.
 */
public class VertxEventLoopGroupProvider implements EventLoopGroupProvider {

    private static final Logger LOGGER = Logger.getLogger(VertxEventLoopGroupProvider.class);

    private final EventLoopGroup eventLoopGroup;
    private final int threadPoolSize;

    public VertxEventLoopGroupProvider(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
        // Vert.x event loop group doesn't expose size directly; use available processors as a reasonable value
        this.threadPoolSize = Runtime.getRuntime().availableProcessors();
        LOGGER.debugf("Created VertxEventLoopGroupProvider with external event loop group: %s",
                eventLoopGroup.getClass().getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends EventLoopGroup> T allocate(Class<T> type) {
        LOGGER.debugf("Allocating event loop group for type %s — returning shared Vert.x event loop group", type.getName());
        return (T) eventLoopGroup;
    }

    @Override
    public int threadPoolSize() {
        return threadPoolSize;
    }

    /**
     * No-op: the event loop group is externally managed by Vert.x.
     */
    @Override
    public Future<Boolean> release(EventExecutorGroup eventLoopGroup, long quietPeriod, long timeout, TimeUnit unit) {
        LOGGER.debug("Release requested for externally managed event loop group — no-op");
        return new SucceededFuture<>(ImmediateEventExecutor.INSTANCE, true);
    }

    /**
     * No-op: the event loop group is externally managed by Vert.x.
     */
    @Override
    public Future<Boolean> shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {
        LOGGER.debug("Shutdown requested for externally managed event loop group — no-op");
        return new SucceededFuture<>(ImmediateEventExecutor.INSTANCE, true);
    }
}
