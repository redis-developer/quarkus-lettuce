package io.quarkus.redis.runtime.client.lettuce.key;

import io.quarkus.redis.datasource.keys.CopyArgs;
import io.quarkus.redis.datasource.keys.ExpireArgs;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.runtime.client.lettuce.ArgTokenCursor;

/**
 * Converters bridging Quarkus Key Command argument types to their Lettuce equivalents.
 */
public final class LettuceKeyCommandsConverters {

    private LettuceKeyCommandsConverters() {
        // Utility class
    }

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

    public static io.lettuce.core.CopyArgs toLettuceCopyArgs(CopyArgs quarkus) {
        io.lettuce.core.CopyArgs lettuce = new io.lettuce.core.CopyArgs();
        Iterable<Object> tokens = quarkus.toArgs();
        var cursor = new ArgTokenCursor(tokens);
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "DB" -> lettuce.destinationDb(cursor.nextLong(token));
                case "REPLACE" -> lettuce.replace(true);
                default -> throw new IllegalStateException("Unexpected CopyArgs token: " + token);
            }
        }
        return lettuce;
    }

    public static io.lettuce.core.KeyScanArgs toLettuceKeyScanArgs(KeyScanArgs quarkus) {
        io.lettuce.core.KeyScanArgs lettuce = new io.lettuce.core.KeyScanArgs();
        Iterable<String> tokens = quarkus.toArgs();
        var cursor = new ArgTokenCursor(tokens);
        while (cursor.hasNext()) {
            String token = cursor.next();
            switch (token) {
                case "MATCH" -> lettuce.match(cursor.nextValue(token));
                case "COUNT" -> lettuce.limit(cursor.nextLong(token));
                case "TYPE" -> lettuce.type(cursor.nextValue(token).toLowerCase());
                default -> throw new IllegalStateException("Unexpected KeyScanArgs token: " + token);
            }
        }
        return lettuce;
    }

}
