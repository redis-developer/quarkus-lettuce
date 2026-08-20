package io.quarkus.redis.runtime.client.lettuce.set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;

/**
 * Unit tests for {@link LettuceSetCommandsConverters}, without a running Redis.
 */
class LettuceSetCommandsConvertersTest {

    // ---------------------------------------------------------------- ScanArgs

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
        assertThat(renderScanArgs(new ScanArgs().match("keep:*"))).containsExactly("MATCH", "keep:*");
    }

    @Test
    void matchAndCount() {
        assertThat(renderScanArgs(new ScanArgs().count(7).match("keep:*")))
                .containsExactly("MATCH", "keep:*", "COUNT", "7");
    }

    @Test
    void matchBeforeCountIsOrderIndependent() {
        assertThat(renderScanArgs(scanArgs("MATCH", "keep:*", "COUNT", "5")))
                .containsExactly("MATCH", "keep:*", "COUNT", "5");
    }

    @Test
    void danglingScanKeywordWithoutValueIsRejected() {
        ScanArgs dangling = scanArgs("COUNT", "5", "MATCH");
        assertThatThrownBy(() -> LettuceSetCommandsConverters.toLettuceScanArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: MATCH");
    }

    @Test
    void unknownScanTokenIsRejected() {
        ScanArgs unknown = scanArgs("COUNT", "5", "NOVALUES");
        assertThatThrownBy(() -> LettuceSetCommandsConverters.toLettuceScanArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected ScanArgs token: NOVALUES");
    }

    // ---------------------------------------------------------------- SortArgs

    @Test
    void emptySortArgsSetsNothing() {
        assertThat(renderSortArgs(new SortArgs())).isEmpty();
    }

    @Test
    void ascending() {
        assertThat(renderSortArgs(new SortArgs().ascending())).containsExactly("ASC");
    }

    @Test
    void descending() {
        assertThat(renderSortArgs(new SortArgs().descending())).containsExactly("DESC");
    }

    @Test
    void alphaOnly() {
        assertThat(renderSortArgs(new SortArgs().alpha())).containsExactly("ALPHA");
    }

    @Test
    void byOnly() {
        assertThat(renderSortArgs(new SortArgs().by("weight_*"))).containsExactly("BY", "weight_*");
    }

    @Test
    void limitWithOffsetAndCount() {
        assertThat(renderSortArgs(new SortArgs().limit(2, 5))).containsExactly("LIMIT", "2", "5");
    }

    @Test
    void limitWithCountOnlyDefaultsOffsetToZero() {
        assertThat(renderSortArgs(new SortArgs().limit(7))).containsExactly("LIMIT", "0", "7");
    }

    /** {@code SortArgs.Limit} emits a single value — the count — when its offset is -1. */
    @Test
    void limitWithASingleValueIsTreatedAsACount() {
        assertThat(renderSortArgs(new SortArgs().limit(SortArgs.Limit.of(-1, 4))))
                .containsExactly("LIMIT", "0", "4");
    }

    @Test
    void limitFollowedByAKeywordIsNotMisread() {
        assertThat(renderSortArgs(sortArgs("LIMIT", "9", "ALPHA")))
                .containsExactly("LIMIT", "0", "9", "ALPHA");
    }

    @Test
    void multipleGetPatterns() {
        assertThat(renderSortArgs(new SortArgs().get("#").get("data_*")))
                .containsExactly("GET", "#", "GET", "data_*");
    }

    @Test
    void allSortOptionsCombined() {
        assertThat(renderSortArgs(new SortArgs()
                .by("weight_*")
                .limit(1, 3)
                .get("#")
                .descending()
                .alpha()))
                .containsExactly("BY", "weight_*", "GET", "#", "LIMIT", "1", "3", "DESC", "ALPHA");
    }

    @Test
    void sortTokenOrderIsIndependent() {
        assertThat(renderSortArgs(sortArgs("ALPHA", "DESC", "GET", "#", "BY", "w_*")))
                .containsExactly("BY", "w_*", "GET", "#", "DESC", "ALPHA");
    }

    @Test
    void danglingSortKeywordWithoutValueIsRejected() {
        SortArgs dangling = sortArgs("ALPHA", "BY");
        assertThatThrownBy(() -> LettuceSetCommandsConverters.toLettuceSortArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: BY");
    }

    @Test
    void danglingLimitWithoutValueIsRejected() {
        SortArgs dangling = sortArgs("ALPHA", "LIMIT");
        assertThatThrownBy(() -> LettuceSetCommandsConverters.toLettuceSortArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: LIMIT");
    }

    @Test
    void unknownSortTokenIsRejected() {
        SortArgs unknown = sortArgs("ALPHA", "STORE");
        assertThatThrownBy(() -> LettuceSetCommandsConverters.toLettuceSortArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected SortArgs token: STORE");
    }

    // ------------------------------------------------------------------ helpers

    private static String[] renderScanArgs(ScanArgs quarkus) {
        return renderToTokens(LettuceSetCommandsConverters.toLettuceScanArgs(quarkus)::build);
    }

    private static String[] renderSortArgs(SortArgs quarkus) {
        return renderToTokens(LettuceSetCommandsConverters.toLettuceSortArgs(quarkus)::build);
    }

    /**
     * Encodes the command args into a RESP byte buffer and extracts the bulk-string values.
     * This avoids the lossy {@code toCommandString()} rendering that base64-encodes or
     * mangles byte-typed values such as the SCAN {@code MATCH} pattern.
     */
    private static String[] renderToTokens(java.util.function.Consumer<CommandArgs<String, String>> builder) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        builder.accept(args);
        io.netty.buffer.ByteBuf buf = io.netty.buffer.ByteBufAllocator.DEFAULT.buffer();
        try {
            args.encode(buf);
            String wire = buf.toString(java.nio.charset.StandardCharsets.UTF_8);
            String[] lines = wire.split("\r\n");
            java.util.List<String> tokens = new java.util.ArrayList<>();
            for (int i = 0; i < lines.length; i++) {
                if (lines[i].startsWith("$") && i + 1 < lines.length) {
                    tokens.add(lines[++i]);
                } else if (lines[i].startsWith(":")) {
                    tokens.add(lines[i].substring(1));
                }
            }
            return tokens.toArray(new String[0]);
        } finally {
            buf.release();
        }
    }

    /** A {@link ScanArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static ScanArgs scanArgs(String... tokens) {
        return new ScanArgs() {
            @Override
            public List<String> toArgs() {
                return List.of(tokens);
            }
        };
    }

    /** A {@link SortArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static SortArgs sortArgs(String... tokens) {
        return new SortArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of(tokens);
            }
        };
    }
}
