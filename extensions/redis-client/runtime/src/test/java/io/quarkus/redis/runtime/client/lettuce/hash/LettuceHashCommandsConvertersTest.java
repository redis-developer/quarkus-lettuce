package io.quarkus.redis.runtime.client.lettuce.hash;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ScanArgs;

/**
 * Unit tests for {@link LettuceHashCommandsConverters}, exercising the Quarkus → Lettuce
 * {@link ScanArgs} conversion without a running Redis instance.
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
    void matchBeforeCountIsOrderIndependent() {
        ScanArgs reordered = scanArgs("MATCH", "keep:*", "COUNT", "5");
        io.lettuce.core.ScanArgs args = LettuceHashCommandsConverters.toLettuceScanArgs(reordered);
        assertThat(count(args)).isEqualTo(5L);
        assertThat(match(args)).isEqualTo("keep:*".getBytes(UTF_8));
    }

    @Test
    void danglingKeywordWithoutValueIsRejected() {
        ScanArgs dangling = scanArgs("COUNT", "5", "MATCH");
        assertThatThrownBy(() -> LettuceHashCommandsConverters.toLettuceScanArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: MATCH");
    }

    @Test
    void unknownTokenIsRejected() {
        ScanArgs unknown = scanArgs("COUNT", "5", "NOVALUES");
        assertThatThrownBy(() -> LettuceHashCommandsConverters.toLettuceScanArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected ScanArgs token: NOVALUES");
    }

    /** A {@link ScanArgs} emitting exactly {@code tokens}, to cover shapes the built-in setters cannot produce. */
    private static ScanArgs scanArgs(String... tokens) {
        return new ScanArgs() {
            @Override
            public List<String> toArgs() {
                return List.of(tokens);
            }
        };
    }
}
