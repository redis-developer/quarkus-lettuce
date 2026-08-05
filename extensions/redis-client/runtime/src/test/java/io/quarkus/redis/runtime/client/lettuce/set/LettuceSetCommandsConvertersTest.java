package io.quarkus.redis.runtime.client.lettuce.set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ScanArgs;
import io.quarkus.redis.datasource.SortArgs;

/**
 * Unit tests for {@link LettuceSetCommandsConverters}, without a running Redis. The Lettuce argument
 * classes expose no getters, so the assertions read their private fields reflectively.
 */
class LettuceSetCommandsConvertersTest {

    // --- ScanArgs ---

    @Test
    void emptyScanArgsSetsNothing() {
        io.lettuce.core.ScanArgs args = LettuceSetCommandsConverters.toLettuceScanArgs(new ScanArgs());
        assertThat(count(args)).isNull();
        assertThat(match(args)).isNull();
    }

    @Test
    void countOnly() {
        io.lettuce.core.ScanArgs args = LettuceSetCommandsConverters.toLettuceScanArgs(new ScanArgs().count(42));
        assertThat(count(args)).isEqualTo(42L);
        assertThat(match(args)).isNull();
    }

    @Test
    void matchOnly() {
        io.lettuce.core.ScanArgs args = LettuceSetCommandsConverters.toLettuceScanArgs(new ScanArgs().match("keep:*"));
        assertThat(count(args)).isNull();
        assertThat(match(args)).isEqualTo("keep:*".getBytes(UTF_8));
    }

    @Test
    void matchAndCount() {
        io.lettuce.core.ScanArgs args = LettuceSetCommandsConverters
                .toLettuceScanArgs(new ScanArgs().count(7).match("keep:*"));
        assertThat(count(args)).isEqualTo(7L);
        assertThat(match(args)).isEqualTo("keep:*".getBytes(UTF_8));
    }

    @Test
    void matchBeforeCountIsOrderIndependent() {
        io.lettuce.core.ScanArgs args = LettuceSetCommandsConverters
                .toLettuceScanArgs(scanArgs("MATCH", "keep:*", "COUNT", "5"));
        assertThat(count(args)).isEqualTo(5L);
        assertThat(match(args)).isEqualTo("keep:*".getBytes(UTF_8));
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

    // --- SortArgs ---

    @Test
    void emptySortArgsSetsNothing() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(new SortArgs());
        assertThat(by(args)).isNull();
        assertThat(get(args)).isNullOrEmpty();
        // Lettuce initializes the field to Limit.unlimited() rather than leaving it null.
        assertThat(limit(args).isLimited()).isFalse();
        assertThat(alpha(args)).isFalse();
        assertThat(order(args)).isNull();
    }

    @Test
    void ascending() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(new SortArgs().ascending());
        assertThat(order(args)).isEqualTo("ASC");
    }

    @Test
    void descending() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(new SortArgs().descending());
        assertThat(order(args)).isEqualTo("DESC");
    }

    @Test
    void alphaOnly() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(new SortArgs().alpha());
        assertThat(alpha(args)).isTrue();
    }

    @Test
    void byOnly() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(new SortArgs().by("weight_*"));
        assertThat(by(args)).isEqualTo("weight_*");
    }

    @Test
    void limitWithOffsetAndCount() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(new SortArgs().limit(2, 5));
        assertThat(limit(args).getOffset()).isEqualTo(2);
        assertThat(limit(args).getCount()).isEqualTo(5);
    }

    @Test
    void limitWithCountOnlyDefaultsOffsetToZero() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(new SortArgs().limit(7));
        assertThat(limit(args).getOffset()).isZero();
        assertThat(limit(args).getCount()).isEqualTo(7);
    }

    @Test
    void limitWithASingleValueIsTreatedAsACount() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters
                .toLettuceSortArgs(new SortArgs().limit(SortArgs.Limit.of(-1, 4)));
        assertThat(limit(args).getOffset()).isZero();
        assertThat(limit(args).getCount()).isEqualTo(4);
    }

    @Test
    void limitFollowedByAKeywordIsNotMisread() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(sortArgs("LIMIT", "9", "ALPHA"));
        assertThat(limit(args).getOffset()).isZero();
        assertThat(limit(args).getCount()).isEqualTo(9);
        assertThat(alpha(args)).isTrue();
    }

    @Test
    void multipleGetPatterns() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters
                .toLettuceSortArgs(new SortArgs().get("#").get("data_*"));
        assertThat(get(args)).containsExactly("#", "data_*");
    }

    @Test
    void allSortOptionsCombined() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters.toLettuceSortArgs(new SortArgs()
                .by("weight_*")
                .limit(1, 3)
                .get("#")
                .descending()
                .alpha());
        assertThat(by(args)).isEqualTo("weight_*");
        assertThat(limit(args).getOffset()).isEqualTo(1);
        assertThat(limit(args).getCount()).isEqualTo(3);
        assertThat(get(args)).containsExactly("#");
        assertThat(order(args)).isEqualTo("DESC");
        assertThat(alpha(args)).isTrue();
    }

    @Test
    void sortTokenOrderIsIndependent() {
        io.lettuce.core.SortArgs args = LettuceSetCommandsConverters
                .toLettuceSortArgs(sortArgs("ALPHA", "DESC", "GET", "#", "BY", "w_*"));
        assertThat(by(args)).isEqualTo("w_*");
        assertThat(get(args)).containsExactly("#");
        assertThat(order(args)).isEqualTo("DESC");
        assertThat(alpha(args)).isTrue();
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

    // --- Helpers ---

    private static Long count(io.lettuce.core.ScanArgs args) {
        return (Long) read(io.lettuce.core.ScanArgs.class, args, "count");
    }

    private static byte[] match(io.lettuce.core.ScanArgs args) {
        return (byte[]) read(io.lettuce.core.ScanArgs.class, args, "match");
    }

    private static String by(io.lettuce.core.SortArgs args) {
        return (String) read(io.lettuce.core.SortArgs.class, args, "by");
    }

    @SuppressWarnings("unchecked")
    private static List<String> get(io.lettuce.core.SortArgs args) {
        return (List<String>) read(io.lettuce.core.SortArgs.class, args, "get");
    }

    private static io.lettuce.core.Limit limit(io.lettuce.core.SortArgs args) {
        return (io.lettuce.core.Limit) read(io.lettuce.core.SortArgs.class, args, "limit");
    }

    private static boolean alpha(io.lettuce.core.SortArgs args) {
        return (Boolean) read(io.lettuce.core.SortArgs.class, args, "alpha");
    }

    private static String order(io.lettuce.core.SortArgs args) {
        Object keyword = read(io.lettuce.core.SortArgs.class, args, "order");
        return keyword == null ? null : keyword.toString();
    }

    private static Object read(Class<?> type, Object args, String name) {
        try {
            Field field = type.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(args);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Cannot read Lettuce " + type.getSimpleName() + "." + name, e);
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
