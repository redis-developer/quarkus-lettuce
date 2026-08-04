package io.quarkus.redis.runtime.client.lettuce.list;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.LPosArgs;
import io.quarkus.redis.datasource.list.Position;

/**
 * Unit tests for {@link LettuceListCommandsConverters}, without a running Redis. The Lettuce argument
 * classes expose no getters, so the assertions read their private fields reflectively.
 */
class LettuceListCommandsConvertersTest {

    // ---------------------------------------------------------------- LPosArgs

    @Test
    void emptyLPosArgsSetsNothing() {
        io.lettuce.core.LPosArgs args = LettuceListCommandsConverters.toLettuceLPosArgs(new LPosArgs());
        assertThat(rank(args)).isNull();
        assertThat(maxlen(args)).isNull();
    }

    @Test
    void rankOnly() {
        io.lettuce.core.LPosArgs args = LettuceListCommandsConverters.toLettuceLPosArgs(new LPosArgs().rank(2));
        assertThat(rank(args)).isEqualTo(2L);
        assertThat(maxlen(args)).isNull();
    }

    @Test
    void negativeRankIsPreserved() {
        io.lettuce.core.LPosArgs args = LettuceListCommandsConverters.toLettuceLPosArgs(new LPosArgs().rank(-1));
        assertThat(rank(args)).isEqualTo(-1L);
    }

    @Test
    void maxlenOnly() {
        io.lettuce.core.LPosArgs args = LettuceListCommandsConverters.toLettuceLPosArgs(new LPosArgs().maxlen(10));
        assertThat(rank(args)).isNull();
        assertThat(maxlen(args)).isEqualTo(10L);
    }

    @Test
    void rankAndMaxlen() {
        io.lettuce.core.LPosArgs args = LettuceListCommandsConverters
                .toLettuceLPosArgs(new LPosArgs().rank(3).maxlen(100));
        assertThat(rank(args)).isEqualTo(3L);
        assertThat(maxlen(args)).isEqualTo(100L);
    }

    @Test
    void maxlenBeforeRankIsOrderIndependent() {
        io.lettuce.core.LPosArgs args = LettuceListCommandsConverters
                .toLettuceLPosArgs(lPosArgs("MAXLEN", "100", "RANK", "3"));
        assertThat(rank(args)).isEqualTo(3L);
        assertThat(maxlen(args)).isEqualTo(100L);
    }

    @Test
    void danglingLPosKeywordWithoutValueIsRejected() {
        LPosArgs dangling = lPosArgs("MAXLEN", "10", "RANK");
        assertThatThrownBy(() -> LettuceListCommandsConverters.toLettuceLPosArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: RANK");
    }

    @Test
    void unknownLPosTokenIsRejected() {
        LPosArgs unknown = lPosArgs("RANK", "1", "COUNT");
        assertThatThrownBy(() -> LettuceListCommandsConverters.toLettuceLPosArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected LPosArgs token: COUNT");
    }

    // ---------------------------------------------------------------- SortArgs

    @Test
    void emptySortArgsSetsNothing() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters.toLettuceSortArgs(new SortArgs());
        assertThat(by(args)).isNull();
        assertThat(get(args)).isNullOrEmpty();
        // Lettuce initialises the field to Limit.unlimited() rather than leaving it null.
        assertThat(limit(args).isLimited()).isFalse();
        assertThat(alpha(args)).isFalse();
        assertThat(order(args)).isNull();
    }

    @Test
    void ascending() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters.toLettuceSortArgs(new SortArgs().ascending());
        assertThat(order(args)).isEqualTo("ASC");
    }

    @Test
    void descending() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters.toLettuceSortArgs(new SortArgs().descending());
        assertThat(order(args)).isEqualTo("DESC");
    }

    @Test
    void alphaOnly() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters.toLettuceSortArgs(new SortArgs().alpha());
        assertThat(alpha(args)).isTrue();
    }

    @Test
    void byOnly() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters.toLettuceSortArgs(new SortArgs().by("weight_*"));
        assertThat(by(args)).isEqualTo("weight_*");
    }

    @Test
    void limitWithOffsetAndCount() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters.toLettuceSortArgs(new SortArgs().limit(2, 5));
        assertThat(limit(args)).isNotNull();
        assertThat(limit(args).getOffset()).isEqualTo(2);
        assertThat(limit(args).getCount()).isEqualTo(5);
    }

    @Test
    void limitWithCountOnlyDefaultsOffsetToZero() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters.toLettuceSortArgs(new SortArgs().limit(7));
        assertThat(limit(args).getOffset()).isZero();
        assertThat(limit(args).getCount()).isEqualTo(7);
    }

    /** {@code SortArgs.Limit} emits a single value — the count — when its offset is -1. */
    @Test
    void limitWithASingleValueIsTreatedAsACount() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters
                .toLettuceSortArgs(new SortArgs().limit(SortArgs.Limit.of(-1, 4)));
        assertThat(limit(args).getOffset()).isZero();
        assertThat(limit(args).getCount()).isEqualTo(4);
    }

    @Test
    void limitFollowedByAKeywordIsNotMisread() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters
                .toLettuceSortArgs(sortArgs("LIMIT", "9", "ALPHA"));
        assertThat(limit(args).getOffset()).isZero();
        assertThat(limit(args).getCount()).isEqualTo(9);
        assertThat(alpha(args)).isTrue();
    }

    @Test
    void multipleGetPatterns() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters
                .toLettuceSortArgs(new SortArgs().get("#").get("data_*"));
        assertThat(get(args)).containsExactly("#", "data_*");
    }

    @Test
    void allSortOptionsCombined() {
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters.toLettuceSortArgs(new SortArgs()
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
        io.lettuce.core.SortArgs args = LettuceListCommandsConverters
                .toLettuceSortArgs(sortArgs("ALPHA", "DESC", "GET", "#", "BY", "w_*"));
        assertThat(by(args)).isEqualTo("w_*");
        assertThat(get(args)).containsExactly("#");
        assertThat(order(args)).isEqualTo("DESC");
        assertThat(alpha(args)).isTrue();
    }

    @Test
    void danglingSortKeywordWithoutValueIsRejected() {
        SortArgs dangling = sortArgs("ALPHA", "BY");
        assertThatThrownBy(() -> LettuceListCommandsConverters.toLettuceSortArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: BY");
    }

    @Test
    void danglingLimitWithoutValueIsRejected() {
        SortArgs dangling = sortArgs("ALPHA", "LIMIT");
        assertThatThrownBy(() -> LettuceListCommandsConverters.toLettuceSortArgs(dangling))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: LIMIT");
    }

    @Test
    void unknownSortTokenIsRejected() {
        SortArgs unknown = sortArgs("ALPHA", "STORE");
        assertThatThrownBy(() -> LettuceListCommandsConverters.toLettuceSortArgs(unknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected SortArgs token: STORE");
    }

    // ---------------------------------------------------- LMoveArgs / LMPopArgs

    @Test
    void lMoveArgsCoverEveryPositionPair() {
        assertThat(lMovePositions(Position.LEFT, Position.LEFT)).containsExactly("LEFT", "LEFT");
        assertThat(lMovePositions(Position.LEFT, Position.RIGHT)).containsExactly("LEFT", "RIGHT");
        assertThat(lMovePositions(Position.RIGHT, Position.LEFT)).containsExactly("RIGHT", "LEFT");
        assertThat(lMovePositions(Position.RIGHT, Position.RIGHT)).containsExactly("RIGHT", "RIGHT");
    }

    @Test
    void lMPopArgsWithoutCount() {
        io.lettuce.core.LMPopArgs left = LettuceListCommandsConverters.toLettuceLMPopArgs(Position.LEFT);
        assertThat(direction(left)).isEqualTo("LEFT");
        assertThat(count(left)).isNull();

        io.lettuce.core.LMPopArgs right = LettuceListCommandsConverters.toLettuceLMPopArgs(Position.RIGHT);
        assertThat(direction(right)).isEqualTo("RIGHT");
        assertThat(count(right)).isNull();
    }

    @Test
    void lMPopArgsWithCount() {
        io.lettuce.core.LMPopArgs args = LettuceListCommandsConverters.toLettuceLMPopArgs(Position.RIGHT, 3);
        assertThat(direction(args)).isEqualTo("RIGHT");
        assertThat(count(args)).isEqualTo(3L);
    }

    // ------------------------------------------------------------------ helpers

    private static Long rank(io.lettuce.core.LPosArgs args) {
        return (Long) read(io.lettuce.core.LPosArgs.class, args, "rank");
    }

    private static Long maxlen(io.lettuce.core.LPosArgs args) {
        return (Long) read(io.lettuce.core.LPosArgs.class, args, "maxlen");
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
        return keyword(read(io.lettuce.core.SortArgs.class, args, "order"));
    }

    private static String direction(io.lettuce.core.LMPopArgs args) {
        return keyword(read(io.lettuce.core.LMPopArgs.class, args, "direction"));
    }

    private static Long count(io.lettuce.core.LMPopArgs args) {
        return (Long) read(io.lettuce.core.LMPopArgs.class, args, "count");
    }

    private static List<String> lMovePositions(Position source, Position destination) {
        io.lettuce.core.LMoveArgs args = LettuceListCommandsConverters.toLettuceLMoveArgs(source, destination);
        return List.of(
                keyword(read(io.lettuce.core.LMoveArgs.class, args, "source")),
                keyword(read(io.lettuce.core.LMoveArgs.class, args, "destination")));
    }

    private static String keyword(Object protocolKeyword) {
        return protocolKeyword == null ? null : protocolKeyword.toString();
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

    /** An {@link LPosArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static LPosArgs lPosArgs(String... tokens) {
        return new LPosArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of((Object[]) tokens);
            }
        };
    }

    /** A {@link SortArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static SortArgs sortArgs(String... tokens) {
        return new SortArgs() {
            @Override
            public List<Object> toArgs() {
                return List.of((Object[]) tokens);
            }
        };
    }
}
