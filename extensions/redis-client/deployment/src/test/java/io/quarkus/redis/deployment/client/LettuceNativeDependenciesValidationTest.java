package io.quarkus.redis.deployment.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.quarkus.runtime.configuration.ConfigurationException;

/**
 * Unit tests for {@link LettuceProcessor#validateNativeDependencies(boolean, boolean, boolean)}
 * and its message-building helper.
 * <p>
 * The {@code @BuildStep} that calls them runs only on native builds, which can't be triggered
 * from {@link io.quarkus.test.QuarkusUnitTest}; covering the methods directly verifies the
 * backend gating, the throw wiring and the dependency-by-dependency message formatting.
 */
public class LettuceNativeDependenciesValidationTest {

    @Test
    void returnsNullWhenBothPresent() {
        assertThat(LettuceProcessor.buildNativeDependenciesError(true, true)).isNull();
    }

    @Test
    void reportsLatencyUtilsWhenOnlyHdrHistogramPresent() {
        String message = LettuceProcessor.buildNativeDependenciesError(false, true);
        assertThat(message).isNotNull();
        assertThat(message).contains("org.latencyutils:LatencyUtils", "org.hdrhistogram:HdrHistogram");
        assertThat(message).contains("<groupId>org.latencyutils</groupId>",
                "<artifactId>LatencyUtils</artifactId>");
        assertThat(message).doesNotContain("<groupId>org.hdrhistogram</groupId>");
    }

    @Test
    void reportsHdrHistogramWhenOnlyLatencyUtilsPresent() {
        String message = LettuceProcessor.buildNativeDependenciesError(true, false);
        assertThat(message).isNotNull();
        assertThat(message).contains("<groupId>org.hdrhistogram</groupId>",
                "<artifactId>HdrHistogram</artifactId>");
        assertThat(message).doesNotContain("<groupId>org.latencyutils</groupId>");
    }

    @Test
    void reportsBothWhenNonePresent() {
        String message = LettuceProcessor.buildNativeDependenciesError(false, false);
        assertThat(message).isNotNull();
        assertThat(message).contains("<groupId>org.latencyutils</groupId>",
                "<artifactId>LatencyUtils</artifactId>",
                "<groupId>org.hdrhistogram</groupId>",
                "<artifactId>HdrHistogram</artifactId>");
        assertThat(message).contains("native image");
    }

    @Test
    void skipsValidationForVertxBackendEvenWhenDepsMissing() {
        assertThatCode(() -> LettuceProcessor.validateNativeDependencies(false, false, false))
                .doesNotThrowAnyException();
    }

    @Test
    void doesNotThrowForLettuceBackendWhenBothDepsPresent() {
        assertThatCode(() -> LettuceProcessor.validateNativeDependencies(true, true, true))
                .doesNotThrowAnyException();
    }

    @Test
    void throwsForLettuceBackendWhenLatencyUtilsMissing() {
        assertThatThrownBy(() -> LettuceProcessor.validateNativeDependencies(true, false, true))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("<artifactId>LatencyUtils</artifactId>")
                .hasMessageNotContaining("<artifactId>HdrHistogram</artifactId>");
    }

    @Test
    void throwsForLettuceBackendWhenHdrHistogramMissing() {
        assertThatThrownBy(() -> LettuceProcessor.validateNativeDependencies(true, true, false))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("<artifactId>HdrHistogram</artifactId>")
                .hasMessageNotContaining("<artifactId>LatencyUtils</artifactId>");
    }

    @Test
    void throwsForLettuceBackendWhenBothDepsMissing() {
        assertThatThrownBy(() -> LettuceProcessor.validateNativeDependencies(true, false, false))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("<artifactId>LatencyUtils</artifactId>",
                        "<artifactId>HdrHistogram</artifactId>");
    }
}
