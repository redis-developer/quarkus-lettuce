package io.quarkus.redis.runtime.client.lettuce.key;

import java.util.List;

import io.quarkus.redis.datasource.keys.CopyArgs;
import io.quarkus.redis.datasource.keys.ExpireArgs;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;

/**
 * Converters bridging Quarkus Key Command argument types to their Lettuce equivalents.
 * <p>
 * Registers itself with {@link LettuceConverterRegistry} on first use.
 */
public final class LettuceKeyCommandsConverters {

    private static volatile boolean registered;

    private LettuceKeyCommandsConverters() {
        // Utility class
    }

    /**
     * Register all Key Command converters with {@link LettuceConverterRegistry}.
     * Idempotent and thread-safe.
     */
    public static void register() {
        if (registered) {
            return;
        }
        synchronized (LettuceKeyCommandsConverters.class) {
            if (registered) {
                return;
            }
            LettuceConverterRegistry.registerArgConverter(ExpireArgs.class,
                    LettuceKeyCommandsConverters::toLettuceExpireArgs);
            LettuceConverterRegistry.registerArgConverter(CopyArgs.class,
                    LettuceKeyCommandsConverters::toLettuceCopyArgs);
            LettuceConverterRegistry.registerArgConverter(KeyScanArgs.class,
                    LettuceKeyCommandsConverters::toLettuceKeyScanArgs);
            registered = true;
        }
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
                case "NX":
                    lettuce.nx();
                    break;
                case "XX":
                    lettuce.xx();
                    break;
                case "GT":
                    lettuce.gt();
                    break;
                case "LT":
                    lettuce.lt();
                    break;
                default:
                    throw new IllegalStateException("Unexpected ExpireArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Convert a Quarkus {@link CopyArgs} to a Lettuce {@link io.lettuce.core.CopyArgs}.
     */
    public static io.lettuce.core.CopyArgs toLettuceCopyArgs(CopyArgs quarkus) {
        io.lettuce.core.CopyArgs lettuce = new io.lettuce.core.CopyArgs();
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i).toString();
            switch (token) {
                case "DB":
                    lettuce.destinationDb(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "REPLACE":
                    lettuce.replace(true);
                    break;
                default:
                    throw new IllegalStateException("Unexpected CopyArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Convert a Quarkus {@link KeyScanArgs} to a Lettuce {@link io.lettuce.core.KeyScanArgs}.
     */
    public static io.lettuce.core.KeyScanArgs toLettuceKeyScanArgs(KeyScanArgs quarkus) {
        io.lettuce.core.KeyScanArgs lettuce = new io.lettuce.core.KeyScanArgs();
        List<String> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            switch (token) {
                case "MATCH":
                    lettuce.match(tokens.get(++i));
                    break;
                case "COUNT":
                    lettuce.limit(Long.parseLong(tokens.get(++i)));
                    break;
                case "TYPE":
                    lettuce.type(tokens.get(++i).toLowerCase());
                    break;
                default:
                    throw new IllegalStateException("Unexpected KeyScanArgs token: " + token);
            }
        }
        return lettuce;
    }
}
