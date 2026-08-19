package io.quarkus.redis.runtime.client.lettuce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

class ArgTokenCursorTest {

    @Test
    void iteratesKeywordsAndValues() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("MATCH", "foo*", "COUNT", "10"));
        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next()).isEqualTo("MATCH");
        assertThat(cursor.nextValue("MATCH")).isEqualTo("foo*");
        assertThat(cursor.next()).isEqualTo("COUNT");
        assertThat(cursor.nextLong("COUNT")).isEqualTo(10L);
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void nextValueFailsWhenValueIsMissing() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("MATCH"));
        cursor.next();
        assertThatThrownBy(() -> cursor.nextValue("MATCH"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing value for token: MATCH");
    }

    @Test
    void nextLongFailsOnMalformedValue() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("COUNT", "many"));
        cursor.next();
        assertThatThrownBy(() -> cursor.nextLong("COUNT"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Invalid numeric value for token COUNT: many")
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void nextDoubleParsesFollowingValue() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("TIMEOUT", "1.5"));
        cursor.next();
        assertThat(cursor.nextDouble("TIMEOUT")).isEqualTo(1.5d);
    }

    @Test
    void nextDoubleFailsOnMalformedValue() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("TIMEOUT", "soon"));
        cursor.next();
        assertThatThrownBy(() -> cursor.nextDouble("TIMEOUT"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Invalid numeric value for token TIMEOUT: soon")
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void worksWithNonStringTokenLists() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.<Object> of("DB", 2L));
        assertThat(cursor.next()).isEqualTo("DB");
        assertThat(cursor.nextLong("DB")).isEqualTo(2L);
    }

    @Test
    void nextIsNumericDetectsNumericTokenWithoutConsumingIt() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("LIMIT", "5"));
        cursor.next();
        assertThat(cursor.nextIsNumeric()).isTrue();
        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next()).isEqualTo("5");
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void nextIsNumericRecognisesNegativeNumbers() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("RANK", "-3"));
        cursor.next();
        assertThat(cursor.nextIsNumeric()).isTrue();
        assertThat(cursor.nextLong("RANK")).isEqualTo(-3L);
    }

    @Test
    void nextIsNumericReturnsFalseForKeywordAndPreservesIt() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("ALPHA"));
        assertThat(cursor.nextIsNumeric()).isFalse();
        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next()).isEqualTo("ALPHA");
    }

    @Test
    void nextIsNumericReturnsFalseAtEndOfInput() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("ALPHA"));
        cursor.next();
        assertThat(cursor.nextIsNumeric()).isFalse();
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void nextIsNumericIsIdempotent() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("10"));
        assertThat(cursor.nextIsNumeric()).isTrue();
        assertThat(cursor.nextIsNumeric()).isTrue();
        assertThat(cursor.next()).isEqualTo("10");
    }

    /** Only integral values count as numeric — {@code Long.parseLong} rejects floating-point tokens. */
    @Test
    void nextIsNumericReturnsFalseForFloatingPointToken() {
        ArgTokenCursor cursor = new ArgTokenCursor(List.of("1.5"));
        assertThat(cursor.nextIsNumeric()).isFalse();
        assertThat(cursor.next()).isEqualTo("1.5");
    }

}
