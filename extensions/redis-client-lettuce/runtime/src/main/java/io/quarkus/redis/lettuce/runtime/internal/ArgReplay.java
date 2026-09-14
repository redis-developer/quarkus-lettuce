package io.quarkus.redis.lettuce.runtime.internal;

import java.util.List;
import java.util.Set;

import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.RedisCommandExtraArguments;

/**
 * Replays Quarkus command argument tokens verbatim into a Lettuce {@link CommandArgs}.
 * <p>
 * The Quarkus argument classes ({@code SetArgs}, {@code ScanArgs}, {@code SortArgs}, ...) expose their state only
 * through {@link RedisCommandExtraArguments#toArgs()}, which returns the wire-format token list the Vert.x backend
 * sends unchanged. Appending those same tokens to the Lettuce command makes both backends byte-identical on the wire
 * and keeps the Lettuce backend independent of the keywords each Quarkus class emits: a new option reaches Redis
 * without any change here.
 * <p>
 * Tokens are passed to {@link CommandArgs#add(Object)}, which handles {@code String}, boxed numbers, {@code byte[]}
 * and {@code ProtocolKeyword} and falls back to {@code toString()} for anything else. Validation happens in the
 * Quarkus builders, as it does for the Vert.x backend; Redis rejects whatever slips through.
 */
public final class ArgReplay {

    private ArgReplay() {
        // Utility class
    }

    /**
     * Appends every token of {@code quarkus.toArgs()} to {@code args}, unchanged and in order.
     *
     * @param quarkus the Quarkus extra arguments
     * @param args the Lettuce command arguments being built
     */
    public static <K, V> void replay(RedisCommandExtraArguments quarkus, CommandArgs<K, V> args) {
        replay(quarkus.toArgs(), args);
    }

    /**
     * Appends every token of {@code tokens} to {@code args}, unchanged and in order.
     * <p>
     * Used for argument classes such as {@code ScanArgs} that expose {@code toArgs()} without implementing
     * {@link RedisCommandExtraArguments}.
     *
     * @param tokens the wire-format tokens
     * @param args the Lettuce command arguments being built
     */
    public static <K, V> void replay(List<?> tokens, CommandArgs<K, V> args) {
        for (Object token : tokens) {
            args.add(token);
        }
    }

    /**
     * Appends every token of {@code quarkus.toArgs()} to {@code args} except the {@code String} tokens contained in
     * {@code skip}, compared case-sensitively.
     * <p>
     * This exists for keywords that Lettuce appends itself, such as {@code GET} in {@code setGet}, which would
     * otherwise be sent twice. Only use it with argument classes whose tokens are keywords and numbers, never user
     * values, since any matching token is dropped regardless of position.
     *
     * @param quarkus the Quarkus extra arguments
     * @param args the Lettuce command arguments being built
     * @param skip the keyword tokens to leave out
     */
    public static <K, V> void replayExcept(RedisCommandExtraArguments quarkus, CommandArgs<K, V> args,
            Set<String> skip) {
        for (Object token : quarkus.toArgs()) {
            if (!(token instanceof String s && skip.contains(s))) {
                args.add(token);
            }
        }
    }

}
