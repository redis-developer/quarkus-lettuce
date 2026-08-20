package io.quarkus.redis.runtime.client.lettuce;

import java.util.Iterator;

/**
 * Cursor over a Quarkus argument token list (the output of {@code toArgs()}) used when
 * converting Quarkus command argument types to their Lettuce equivalents.
 * <p>
 * The Quarkus argument classes do not expose their state through getters, so converters
 * parse the wire-format token list instead — a linear sequence of keywords, each optionally
 * followed by a value token (e.g. {@code MATCH pattern COUNT 10}). The cursor owns the
 * iteration state and provides helpers to consume the value token that follows a keyword,
 * failing with a descriptive message if it is missing or malformed.
 */
public final class ArgTokenCursor {

    private final Iterator<?> tokens;

    private Object peeked;

    public ArgTokenCursor(Iterable<?> args) {
        this.tokens = args.iterator();
    }

    /**
     * Whether any tokens remain.
     */
    public boolean hasNext() {
        return peeked != null || tokens.hasNext();
    }

    /**
     * Consume and return the next token, typically a keyword such as {@code MATCH} or {@code COUNT}.
     */
    public String next() {
        if (peeked != null) {
            String value = peeked.toString();
            peeked = null;
            return value;
        }
        return tokens.next().toString();
    }

    /**
     * Consume and return the value token that follows a keyword (e.g. the pattern after {@code MATCH}).
     *
     * @param keyword the keyword whose value is expected next, used in the error message
     * @return the value token
     * @throws IllegalStateException if the token list ends before the value
     */
    public String nextValue(String keyword) {
        if (!hasNext()) {
            throw new IllegalStateException("Missing value for token: " + keyword);
        }
        return next();
    }

    /**
     * Consume and parse the numeric value token that follows a keyword (e.g. the count after {@code COUNT}).
     *
     * @param keyword the keyword whose value is expected next, used in the error message
     * @return the value token parsed as a {@code long}
     * @throws IllegalStateException if the token list ends before the value or the value is not a valid {@code long}
     */
    public long nextLong(String keyword) {
        String value = nextValue(keyword);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid numeric value for token " + keyword + ": " + value, e);
        }
    }

    /**
     * Consume and parse the floating-point value token that follows a keyword (e.g. a score or timeout).
     *
     * @param keyword the keyword whose value is expected next, used in the error message
     * @return the value token parsed as a {@code double}
     * @throws IllegalStateException if the token list ends before the value or the value is not a valid {@code double}
     */
    public double nextDouble(String keyword) {
        String value = nextValue(keyword);
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid numeric value for token " + keyword + ": " + value, e);
        }
    }

    /**
     * Whether the next token exists and parses as a {@code long}, without consuming it.
     * Used to disambiguate keywords whose trailing value tokens are optional.
     */
    public boolean nextIsNumeric() {
        if (peeked == null) {
            if (!tokens.hasNext()) {
                return false;
            }
            peeked = tokens.next();
        }
        try {
            Long.parseLong(peeked.toString());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Whether the next token exists and parses as a {@code double}, without consuming it.
     * Used to consume a run of floating-point value tokens (e.g. the {@code WEIGHTS} list)
     * up to the next keyword.
     */
    public boolean nextIsDouble() {
        if (peeked == null) {
            if (!tokens.hasNext()) {
                return false;
            }
            peeked = tokens.next();
        }
        try {
            Double.parseDouble(peeked.toString());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}
