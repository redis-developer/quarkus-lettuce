package io.quarkus.redis.runtime.client.lettuce.key;

import java.util.Iterator;

import io.quarkus.redis.datasource.keys.CopyArgs;
import io.quarkus.redis.datasource.keys.ExpireArgs;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;

/**
 * Converters bridging Quarkus Key Command argument types to their Lettuce equivalents.
 * <p>
 * Registration with {@link LettuceConverterRegistry} happens in this class's static initializer.
 */
public final class LettuceKeyCommandsConverters {

    static {
        LettuceConverterRegistry.registerArgConverter(ExpireArgs.class,
                LettuceKeyCommandsConverters::toLettuceExpireArgs);
        LettuceConverterRegistry.registerArgConverter(CopyArgs.class,
                LettuceKeyCommandsConverters::toLettuceCopyArgs);
        LettuceConverterRegistry.registerArgConverter(KeyScanArgs.class,
                LettuceKeyCommandsConverters::toLettuceKeyScanArgs);
    }

    private LettuceKeyCommandsConverters() {
        // Utility class
    }

    /**
     * Ensures the Key Command converters are registered with {@link LettuceConverterRegistry}.
     * <p>
     * The registration itself runs in this class's static initializer; calling this method simply
     * forces class initialization at a well-defined point. It is therefore idempotent and
     * thread-safe by virtue of the JVM's class-initialization guarantees.
     */
    public static void register() {
        // No-op: registration is performed in the static initializer.
    }

    /**
     * Convert a Quarkus {@link ExpireArgs} to a Lettuce {@link io.lettuce.core.ExpireArgs}.
     * <p>
     * The Quarkus class does not expose its flags through getters, so we parse its
     * wire-format token list ({@link ExpireArgs#toArgs()}) — the stable public contract.
     */
    public static io.lettuce.core.ExpireArgs toLettuceExpireArgs(ExpireArgs quarkus) {
        io.lettuce.core.ExpireArgs lettuce = new io.lettuce.core.ExpireArgs();
        for (Object token : quarkus.toArgs()) {
            switch (token.toString()) {
                case "NX" -> lettuce.nx();
                case "XX" -> lettuce.xx();
                case "GT" -> lettuce.gt();
                case "LT" -> lettuce.lt();
                default -> throw new IllegalStateException("Unexpected ExpireArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Convert a Quarkus {@link CopyArgs} to a Lettuce {@link io.lettuce.core.CopyArgs}.
     */
    public static io.lettuce.core.CopyArgs toLettuceCopyArgs(CopyArgs quarkus) {
        io.lettuce.core.CopyArgs lettuce = new io.lettuce.core.CopyArgs();
        Iterator<Object> tokens = quarkus.toArgs().iterator();
        while (tokens.hasNext()) {
            String token = tokens.next().toString();
            switch (token) {
                case "DB" -> lettuce.destinationDb(nextLong(tokens, token));
                case "REPLACE" -> lettuce.replace(true);
                default -> throw new IllegalStateException("Unexpected CopyArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Convert a Quarkus {@link KeyScanArgs} to a Lettuce {@link io.lettuce.core.KeyScanArgs}.
     */
    public static io.lettuce.core.KeyScanArgs toLettuceKeyScanArgs(KeyScanArgs quarkus) {
        io.lettuce.core.KeyScanArgs lettuce = new io.lettuce.core.KeyScanArgs();
        Iterator<String> tokens = quarkus.toArgs().iterator();
        while (tokens.hasNext()) {
            String token = tokens.next();
            switch (token) {
                case "MATCH" -> lettuce.match(nextToken(tokens, token));
                case "COUNT" -> lettuce.limit(nextLong(tokens, token));
                case "TYPE" -> lettuce.type(nextToken(tokens, token).toLowerCase());
                default -> throw new IllegalStateException("Unexpected KeyScanArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Consume and return the value token that follows a keyword (e.g. the pattern after {@code MATCH}).
     */
    private static String nextToken(Iterator<?> tokens, String token) {
        if (!tokens.hasNext()) {
            throw new IllegalStateException("Missing value for token: " + token);
        }
        return tokens.next().toString();
    }

    /**
     * Consume and parse the numeric value token that follows a keyword (e.g. the count after {@code COUNT}).
     */
    private static long nextLong(Iterator<?> tokens, String token) {
        return Long.parseLong(nextToken(tokens, token));
    }
}
