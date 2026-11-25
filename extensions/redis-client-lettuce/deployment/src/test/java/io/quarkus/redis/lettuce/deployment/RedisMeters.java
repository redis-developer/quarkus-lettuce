package io.quarkus.redis.lettuce.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Reads the Redis meters of a client from a registry, for the tests of the metrics of the Lettuce backend. A meter
 * that does not exist reads as zero: the meters of a client are created by its first command.
 */
public final class RedisMeters {

    /** The values of the four {@code redis.commands.*} meters of a client. */
    public record Commands(long count, long success, long failure, long timed) {

        /** These values after {@code commands} more commands, {@code failed} of which failed. */
        public Commands plus(long commands, long failed) {
            return new Commands(count + commands, success + commands - failed, failure + failed, timed + commands);
        }
    }

    private RedisMeters() {
    }

    public static Commands commands(MeterRegistry registry, String client) {
        return new Commands(counter(registry, "redis.commands.count", client),
                counter(registry, "redis.commands.success", client),
                counter(registry, "redis.commands.failure", client),
                timer(registry, client));
    }

    /**
     * Waits for the meters of a client to read the expected values: Lettuce completes a command before it notifies
     * its listeners, so the caller of a command may be back before the command is recorded.
     */
    public static void awaitCommands(MeterRegistry registry, String client, Commands expected) {
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(commands(registry, client)).isEqualTo(expected));
    }

    /**
     * The value of a {@code redis.pool.*} gauge of the pool of a client, or {@code -1} when there is no such gauge.
     * The pools are tagged as the metrics of Vert.x tag the pool of the Vert.x client.
     */
    public static double pool(MeterRegistry registry, String gauge, String client) {
        Gauge found = registry.find("redis.pool." + gauge).tag("pool.type", "redis").tag("pool.name", client).gauge();
        return found == null ? -1 : found.value();
    }

    /** How many times a connection of the pool of a client was used and returned. */
    public static long poolCompleted(MeterRegistry registry, String client) {
        Counter counter = registry.find("redis.pool.completed").tag("pool.type", "redis").tag("pool.name", client)
                .counter();
        return counter == null ? 0 : (long) counter.count();
    }

    private static long counter(MeterRegistry registry, String name, String client) {
        Counter counter = registry.find(name).tag("client-name", client).counter();
        return counter == null ? 0 : (long) counter.count();
    }

    private static long timer(MeterRegistry registry, String client) {
        Timer timer = registry.find("redis.commands.duration").tag("client-name", client).timer();
        return timer == null ? 0 : timer.count();
    }
}
