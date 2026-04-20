package io.quarkus.redis.runtime.client.lettuce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LettuceConverterRegistry}.
 */
class LettuceConverterRegistryTest {

    @AfterEach
    void cleanup() {
        LettuceConverterRegistry.clear();
    }

    @Test
    void registerAndLookupArgConverter() {
        LettuceConverterRegistry.registerArgConverter(String.class, s -> s.toUpperCase());
        Function<Object, Object> converter = LettuceConverterRegistry.getArgConverter(String.class);
        assertThat(converter).isNotNull();
        assertThat(converter.apply("hello")).isEqualTo("HELLO");
    }

    @Test
    void registerAndLookupResultConverter() {
        LettuceConverterRegistry.registerResultConverter(Integer.class, i -> i * 2);
        Function<Object, Object> converter = LettuceConverterRegistry.getResultConverter(Integer.class);
        assertThat(converter).isNotNull();
        assertThat(converter.apply(5)).isEqualTo(10);
    }

    @Test
    void lookupReturnsNullForUnregistered() {
        assertThat(LettuceConverterRegistry.getArgConverter(Double.class)).isNull();
        assertThat(LettuceConverterRegistry.getResultConverter(Double.class)).isNull();
    }

    @Test
    void convertArgShouldApplyConverter() {
        LettuceConverterRegistry.registerArgConverter(String.class, s -> s.length());
        int result = LettuceConverterRegistry.convertArg("hello");
        assertThat(result).isEqualTo(5);
    }

    @Test
    void convertArgShouldReturnNullForNullInput() {
        String result = LettuceConverterRegistry.convertArg(null);
        assertThat(result).isNull();
    }

    @Test
    void convertArgShouldThrowForUnregistered() {
        assertThatThrownBy(() -> LettuceConverterRegistry.convertArg("no-converter"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No Lettuce converter registered for argument type");
    }

    @Test
    void convertResultShouldApplyConverter() {
        LettuceConverterRegistry.registerResultConverter(Long.class, l -> l.toString());
        String result = LettuceConverterRegistry.convertResult(42L);
        assertThat(result).isEqualTo("42");
    }

    @Test
    void convertResultShouldThrowForUnregistered() {
        assertThatThrownBy(() -> LettuceConverterRegistry.convertResult(3.14))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No Lettuce converter registered for result type");
    }

    @Test
    void clearShouldRemoveAllConverters() {
        LettuceConverterRegistry.registerArgConverter(String.class, s -> s);
        LettuceConverterRegistry.registerResultConverter(Integer.class, i -> i);
        LettuceConverterRegistry.clear();
        assertThat(LettuceConverterRegistry.getArgConverter(String.class)).isNull();
        assertThat(LettuceConverterRegistry.getResultConverter(Integer.class)).isNull();
    }
}
