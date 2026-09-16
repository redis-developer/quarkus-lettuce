package io.quarkus.redis.lettuce.runtime.internal.value;

import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.lettuce.runtime.internal.ArgTokenCursor;

/**
 * Converters bridging Quarkus Value Command argument types to their Lettuce equivalents.
 */
public final class LettuceValueCommandsConverters {

    private LettuceValueCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.SetArgs toLettuceSetArgs(SetArgs quarkus) {
        io.lettuce.core.SetArgs lettuce = new io.lettuce.core.SetArgs();
        var cursor = new ArgTokenCursor(quarkus.toArgs());
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "EX" -> lettuce.ex(cursor.nextLong(token));
                case "EXAT" -> lettuce.exAt(cursor.nextLong(token));
                case "PX" -> lettuce.px(cursor.nextLong(token));
                case "PXAT" -> lettuce.pxAt(cursor.nextLong(token));
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

    public static io.lettuce.core.GetExArgs toLettuceGetExArgs(GetExArgs quarkus) {
        io.lettuce.core.GetExArgs lettuce = new io.lettuce.core.GetExArgs();
        var cursor = new ArgTokenCursor(quarkus.toArgs());
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "EX" -> lettuce.ex(cursor.nextLong(token));
                case "EXAT" -> lettuce.exAt(cursor.nextLong(token));
                case "PX" -> lettuce.px(cursor.nextLong(token));
                case "PXAT" -> lettuce.pxAt(cursor.nextLong(token));
                case "PERSIST" -> lettuce.persist();
                default -> throw new IllegalStateException("Unexpected GetExArgs token: " + token);
            }
        }
        return lettuce;
    }

}
