package io.quarkus.redis.runtime.client.lettuce.list;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.SortArgs;
import io.quarkus.redis.datasource.list.LPosArgs;
import io.quarkus.redis.datasource.list.Position;

/**
 * Unit tests for {@link LettuceListCommandsConverters}.
 */
class LettuceListCommandsConvertersTest {

    // ---------------------------------------------------------------- LPosArgs

    @Test
    void emptyLPosArgsSetsNothing() {
        assertThat(renderLPosArgs(new LPosArgs())).isEmpty();
    }

    @Test
    void rankOnly() {
        assertThat(renderLPosArgs(new LPosArgs().rank(2))).containsExactly("RANK", "2");
    }

    @Test
    void negativeRankIsPreserved() {
        assertThat(renderLPosArgs(new LPosArgs().rank(-1))).containsExactly("RANK", "-1");
    }

    @Test
    void maxlenOnly() {
        assertThat(renderLPosArgs(new LPosArgs().maxlen(10))).containsExactly("MAXLEN", "10");
    }

    @Test
    void rankAndMaxlen() {
        assertThat(renderLPosArgs(new LPosArgs().rank(3).maxlen(100)))
                .containsExactly("MAXLEN", "100", "RANK", "3");
    }

    @Test
    void maxlenBeforeRankIsOrderIndependent() {
        assertThat(renderLPosArgs(lPosArgs("MAXLEN", "100", "RANK", "3")))
                .containsExactly("MAXLEN", "100", "RANK", "3");
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
        assertThat(renderLMoveArgs(Position.LEFT, Position.LEFT)).containsExactly("LEFT", "LEFT");
        assertThat(renderLMoveArgs(Position.LEFT, Position.RIGHT)).containsExactly("LEFT", "RIGHT");
        assertThat(renderLMoveArgs(Position.RIGHT, Position.LEFT)).containsExactly("RIGHT", "LEFT");
        assertThat(renderLMoveArgs(Position.RIGHT, Position.RIGHT)).containsExactly("RIGHT", "RIGHT");
    }

    @Test
    void lMPopArgsWithoutCount() {
        assertThat(renderToTokens(LettuceListCommandsConverters.toLettuceLMPopArgs(Position.LEFT)::build))
                .containsExactly("LEFT");
        assertThat(renderToTokens(LettuceListCommandsConverters.toLettuceLMPopArgs(Position.RIGHT)::build))
                .containsExactly("RIGHT");
    }

    @Test
    void lMPopArgsWithCount() {
        assertThat(renderToTokens(LettuceListCommandsConverters.toLettuceLMPopArgs(Position.RIGHT, 3)::build))
                .containsExactly("RIGHT", "COUNT", "3");
    }

    // ------------------------------------------------------------------ helpers

    private static String[] renderLPosArgs(LPosArgs quarkus) {
        return renderToTokens(LettuceListCommandsConverters.toLettuceLPosArgs(quarkus)::build);
    }

    private static String[] renderSortArgs(SortArgs quarkus) {
        return renderToTokens(LettuceListCommandsConverters.toLettuceSortArgs(quarkus)::build);
    }

    private static String[] renderLMoveArgs(Position source, Position destination) {
        return renderToTokens(LettuceListCommandsConverters.toLettuceLMoveArgs(source, destination)::build);
    }

    private static String[] renderToTokens(java.util.function.Consumer<CommandArgs<String, String>> builder) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        builder.accept(args);
        // CommandArgs.toCommandString() renders tokens space-separated, unquoted
        String rendered = args.toCommandString();
        if (rendered == null || rendered.isEmpty()) {
            return new String[0];
        }
        return rendered.split(" ");
    }

    /** An {@link LPosArgs} emitting exactly {@code tokens}, to cover shapes the setters cannot make. */
    private static LPosArgs lPosArgs(String... tokens) {
        return new LPosArgs() {
            @Override
            public List<Object> toArgs() {
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
