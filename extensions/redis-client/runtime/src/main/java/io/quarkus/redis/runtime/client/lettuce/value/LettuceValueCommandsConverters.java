package io.quarkus.redis.runtime.client.lettuce.value;

import java.util.List;

import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;

/**
 * Converters bridging Quarkus Value Command argument types to their Lettuce equivalents.
 * <p>
 * Registers itself with {@link LettuceConverterRegistry} on first use.
 */
public final class LettuceValueCommandsConverters {

    private static volatile boolean registered;

    private LettuceValueCommandsConverters() {
        // Utility class
    }

    /**
     * Register all Value Command converters with {@link LettuceConverterRegistry}.
     * Idempotent and thread-safe.
     */
    public static void register() {
        if (registered) {
            return;
        }
        synchronized (LettuceValueCommandsConverters.class) {
            if (registered) {
                return;
            }
            LettuceConverterRegistry.registerArgConverter(SetArgs.class,
                    LettuceValueCommandsConverters::toLettuceSetArgs);
            LettuceConverterRegistry.registerArgConverter(GetExArgs.class,
                    LettuceValueCommandsConverters::toLettuceGetExArgs);
            registered = true;
        }
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
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i).toString();
            switch (token) {
                case "EX":
                    lettuce.ex(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "EXAT":
                    lettuce.exAt(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "PX":
                    lettuce.px(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "PXAT":
                    lettuce.pxAt(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "NX":
                    lettuce.nx();
                    break;
                case "XX":
                    lettuce.xx();
                    break;
                case "KEEPTTL":
                    lettuce.keepttl();
                    break;
                case "GET":
                    // Handled via the dedicated setGet() method on the Lettuce API
                    break;
                default:
                    throw new IllegalStateException("Unexpected SetArgs token: " + token);
            }
        }
        return lettuce;
    }

    /**
     * Convert a Quarkus {@link GetExArgs} to a Lettuce {@link io.lettuce.core.GetExArgs}.
     */
    public static io.lettuce.core.GetExArgs toLettuceGetExArgs(GetExArgs quarkus) {
        io.lettuce.core.GetExArgs lettuce = new io.lettuce.core.GetExArgs();
        List<Object> tokens = quarkus.toArgs();
        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i).toString();
            switch (token) {
                case "EX":
                    lettuce.ex(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "EXAT":
                    lettuce.exAt(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "PX":
                    lettuce.px(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "PXAT":
                    lettuce.pxAt(Long.parseLong(tokens.get(++i).toString()));
                    break;
                case "PERSIST":
                    lettuce.persist();
                    break;
                default:
                    throw new IllegalStateException("Unexpected GetExArgs token: " + token);
            }
        }
        return lettuce;
    }
}
