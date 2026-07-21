package io.quarkus.redis.runtime.client.lettuce.hash;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;

/**
 * Unit tests for {@link LettuceHashCommandsConverters}, exercising the Quarkus → Lettuce
 * {@link ScanArgs} conversion without a running Redis instance.
 * <p>
 * Lettuce's {@link io.lettuce.core.ScanArgs} exposes no getters, so the {@code count} ({@link Long})
 * and {@code match} ({@code byte[]}) fields are read reflectively — they are stable across the
 * Lettuce 6.x line the extension depends on.
 */
class LettuceHashCommandsConvertersTest {

    private static Long count(io.lettuce.core.ScanArgs args) {
        return (Long) read(args, "count");
    }

    private static byte[] match(io.lettuce.core.ScanArgs args) {
        return (byte[]) read(args, "match");
    }

    private static Object read(io.lettuce.core.ScanArgs args, String name) {
        try {
            Field field = io.lettuce.core.ScanArgs.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(args);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Cannot read Lettuce ScanArgs." + name, e);
        }
    }

    @Test
    void emptyScanArgsSetsNothing() {
        io.lettuce.core.ScanArgs args = LettuceHashCommandsConverters.toLettuceScanArgs(new ScanArgs());
        assertThat(count(args)).isNull();
        assertThat(match(args)).isNull();
    }

    @Test
    void countOnly() {
        io.lettuce.core.ScanArgs args = LettuceHashCommandsConverters.toLettuceScanArgs(new ScanArgs().count(42));
        assertThat(count(args)).isEqualTo(42L);
        assertThat(match(args)).isNull();
    }

    @Test
    void matchOnly() {
        io.lettuce.core.ScanArgs args = LettuceHashCommandsConverters.toLettuceScanArgs(new ScanArgs().match("keep:*"));
        assertThat(count(args)).isNull();
        assertThat(match(args)).isEqualTo("keep:*".getBytes(UTF_8));
    }

    @Test
    void matchAndCount() {
        io.lettuce.core.ScanArgs args = LettuceHashCommandsConverters
                .toLettuceScanArgs(new ScanArgs().count(7).match("keep:*"));
        assertThat(count(args)).isEqualTo(7L);
        assertThat(match(args)).isEqualTo("keep:*".getBytes(UTF_8));
    }

    @Test
    void registerInstallsScanArgsConverter() {
        LettuceHashCommandsConverters.register();

        Function<Object, Object> converter = LettuceConverterRegistry.getArgConverter(ScanArgs.class);
        assertThat(converter).isNotNull();

        Object converted = converter.apply(new ScanArgs().match("keep:*"));
        assertThat(converted).isInstanceOf(io.lettuce.core.ScanArgs.class);
        assertThat(match((io.lettuce.core.ScanArgs) converted)).isEqualTo("keep:*".getBytes(UTF_8));
    }

    @Test
    void convertArgViaRegistry() {
        LettuceHashCommandsConverters.register();

        io.lettuce.core.ScanArgs converted = LettuceConverterRegistry.convertArg(new ScanArgs().count(10));
        assertThat(count(converted)).isEqualTo(10L);
    }
}
