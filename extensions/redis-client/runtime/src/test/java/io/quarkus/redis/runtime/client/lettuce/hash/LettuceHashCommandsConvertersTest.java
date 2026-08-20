package io.quarkus.redis.runtime.client.lettuce.hash;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.ScanArgs;

/**
 * Unit tests for {@link LettuceHashCommandsConverters}, without a running Redis.
 */
class LettuceHashCommandsConvertersTest {

    @Test
    void emptyScanArgsSetsNothing() {
        assertThat(renderScanArgs(new ScanArgs())).isEmpty();
    }

    @Test
    void countOnly() {
        assertThat(renderScanArgs(new ScanArgs().count(42))).containsExactly("COUNT", "42");
    }

    @Test
    void matchOnly() {
        assertThat(renderScanArgs(new ScanArgs().match("keep:*")))
                .containsExactly("MATCH", base64("keep:*"));
    }

    @Test
    void matchAndCount() {
        assertThat(renderScanArgs(new ScanArgs().count(7).match("keep:*")))
                .containsExactly("MATCH", base64("keep:*"), "COUNT", "7");
    }

    @Test
    void matchBeforeCountIsOrderIndependent() {
        assertThat(renderScanArgs(scanArgs("MATCH", "keep:*", "COUNT", "5")))
                .containsExactly("MATCH", base64("keep:*"), "COUNT", "5");
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

    // ------------------------------------------------------------------ helpers

    private static String[] renderScanArgs(ScanArgs quarkus) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        LettuceHashCommandsConverters.toLettuceScanArgs(quarkus).build(args);
        // CommandArgs.toCommandString() renders tokens space-separated, unquoted
        String rendered = args.toCommandString();
        if (rendered == null || rendered.isEmpty()) {
            return new String[0];
        }
        return rendered.split(" ");
    }

    /** Lettuce stores the MATCH pattern as bytes, which {@code toCommandString()} renders Base64-encoded. */
    private static String base64(String pattern) {
        return Base64.getEncoder().encodeToString(pattern.getBytes(UTF_8));
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
