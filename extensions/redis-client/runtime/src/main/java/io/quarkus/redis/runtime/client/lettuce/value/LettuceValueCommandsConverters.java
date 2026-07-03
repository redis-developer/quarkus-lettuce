package io.quarkus.redis.runtime.client.lettuce.value;

import java.util.Iterator;

import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;

/**
 * Converters bridging Quarkus Value Command argument types to their Lettuce equivalents.
 * <p>
 * Registration with {@link LettuceConverterRegistry} happens in this class's static initializer.
 */
public final class LettuceValueCommandsConverters {

    static {
        LettuceConverterRegistry.registerArgConverter(SetArgs.class,
                LettuceValueCommandsConverters::toLettuceSetArgs);
        LettuceConverterRegistry.registerArgConverter(GetExArgs.class,
                LettuceValueCommandsConverters::toLettuceGetExArgs);
    }

    private LettuceValueCommandsConverters() {
        // Utility class
    }

    /**
     * Ensures the Value Command converters are registered with {@link LettuceConverterRegistry}.
     * <p>
     * The registration itself runs in this class's static initializer; calling this method simply
     * forces class initialization at a well-defined point. It is therefore idempotent and
     * thread-safe by virtue of the JVM's class-initialization guarantees.
     */
    public static void register() {
        // No-op: registration is performed in the static initializer.
    }

    /**
     * Convert a Quarkus {@link SetArgs} to a Lettuce {@link io.lettuce.core.SetArgs}.
     * <p>
     * The Quarkus class does not expose its state through getters, so we parse its
     * wire-format token list ({@link SetArgs#toArgs()}) — the stable public contract.
     * The {@code GET} flag is intentionally ignored: Lettuce dispatches SET with GET
     * through a dedicated command method rather than via {@code SetArgs}.
     */
    public static io.lettuce.core.SetArgs toLettuceSetArgs(SetArgs quarkus) {
        io.lettuce.core.SetArgs lettuce = new io.lettuce.core.SetArgs();
        Iterator<Object> tokens = quarkus.toArgs().iterator();
        while (tokens.hasNext()) {
            String token = tokens.next().toString();
            switch (token) {
                case "EX" -> lettuce.ex(nextLong(tokens, token));
                case "EXAT" -> lettuce.exAt(nextLong(tokens, token));
                case "PX" -> lettuce.px(nextLong(tokens, token));
                case "PXAT" -> lettuce.pxAt(nextLong(tokens, token));
                case "NX" -> lettuce.nx();
                case "XX" -> lettuce.xx();
                case "KEEPTTL" -> lettuce.keepttl();
                // GET is handled via the dedicated setGet() method on the Lettuce API.
                case "GET" -> {
                }
                default -> throw new IllegalStateException("Unexpected SetArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Convert a Quarkus {@link GetExArgs} to a Lettuce {@link io.lettuce.core.GetExArgs}.
     */
    public static io.lettuce.core.GetExArgs toLettuceGetExArgs(GetExArgs quarkus) {
        io.lettuce.core.GetExArgs lettuce = new io.lettuce.core.GetExArgs();
        Iterator<Object> tokens = quarkus.toArgs().iterator();
        while (tokens.hasNext()) {
            String token = tokens.next().toString();
            switch (token) {
                case "EX" -> lettuce.ex(nextLong(tokens, token));
                case "EXAT" -> lettuce.exAt(nextLong(tokens, token));
                case "PX" -> lettuce.px(nextLong(tokens, token));
                case "PXAT" -> lettuce.pxAt(nextLong(tokens, token));
                case "PERSIST" -> lettuce.persist();
                default -> throw new IllegalStateException("Unexpected GetExArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Consume and parse the value token that follows a keyword (e.g. the seconds after {@code EX}).
     */
    private static long nextLong(Iterator<Object> tokens, String token) {
        if (!tokens.hasNext()) {
            throw new IllegalStateException("Missing value for token: " + token);
        }
        return Long.parseLong(tokens.next().toString());
    }
}
