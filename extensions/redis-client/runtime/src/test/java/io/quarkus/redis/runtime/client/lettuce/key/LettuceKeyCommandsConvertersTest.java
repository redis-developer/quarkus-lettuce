package io.quarkus.redis.runtime.client.lettuce.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.keys.CopyArgs;
import io.quarkus.redis.datasource.keys.ExpireArgs;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.datasource.keys.RedisValueType;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;

/**
 * Unit tests for {@link LettuceKeyCommandsConverters}.
 * <p>
 * Each test builds a Quarkus arg object, converts it, then renders the Lettuce args to
 * wire format via {@link CommandArgs#toCommandString()} and asserts the resulting token
 * list matches the Quarkus {@code toArgs()} output.
 */
class LettuceKeyCommandsConvertersTest {

    @Test
    void expireArgsNx() {
        assertThat(renderExpireArgs(new ExpireArgs().nx())).containsExactly("NX");
    }

    @Test
    void expireArgsXx() {
        assertThat(renderExpireArgs(new ExpireArgs().xx())).containsExactly("XX");
    }

    @Test
    void expireArgsGt() {
        assertThat(renderExpireArgs(new ExpireArgs().gt())).containsExactly("GT");
    }

    @Test
    void expireArgsLt() {
        assertThat(renderExpireArgs(new ExpireArgs().lt())).containsExactly("LT");
    }

    @Test
    void expireArgsEmpty() {
        assertThat(renderExpireArgs(new ExpireArgs())).isEmpty();
    }

    @Test
    void copyArgsDb() {
        assertThat(renderCopyArgs(new CopyArgs().destinationDb(2))).containsExactly("DB", "2");
    }

    @Test
    void copyArgsReplace() {
        assertThat(renderCopyArgs(new CopyArgs().replace(true))).containsExactly("REPLACE");
    }

    @Test
    void copyArgsCombined() {
        assertThat(renderCopyArgs(new CopyArgs().destinationDb(3).replace(true)))
                .containsExactly("DB", "3", "REPLACE");
    }

    @Test
    void copyArgsEmpty() {
        assertThat(renderCopyArgs(new CopyArgs())).isEmpty();
    }

    @Test
    void keyScanArgsMatch() {
        assertThat(renderKeyScanArgs(new KeyScanArgs().match("foo:*"))).containsExactly("MATCH", "foo:*");
    }

    @Test
    void keyScanArgsCount() {
        assertThat(renderKeyScanArgs(new KeyScanArgs().count(100L))).containsExactly("COUNT", "100");
    }

    @Test
    void keyScanArgsType() {
        // The Lettuce KeyScanArgs lowercases the TYPE token before sending.
        assertThat(renderKeyScanArgs(new KeyScanArgs().type(RedisValueType.STRING)))
                .containsExactly("TYPE", "string");
    }

    @Test
    void keyScanArgsCombined() {
        // Lettuce emits MATCH, then COUNT, then TYPE; Redis accepts any ordering.
        KeyScanArgs q = new KeyScanArgs().match("user:*").count(50L).type(RedisValueType.HASH);
        assertThat(renderKeyScanArgs(q)).containsExactly("MATCH", "user:*", "COUNT", "50", "TYPE", "hash");
    }

    @Test
    void keyScanArgsEmpty() {
        assertThat(renderKeyScanArgs(new KeyScanArgs())).isEmpty();
    }

    @Test
    void registerIsIdempotent() {
        LettuceKeyCommandsConverters.register();
        LettuceKeyCommandsConverters.register();
        // No exception, no duplicate registration side effects.
        assertThat(renderExpireArgs(new ExpireArgs().nx())).containsExactly("NX");
    }

    @Test
    void registerReinstatesConvertersAfterRegistryCleared() {
        LettuceKeyCommandsConverters.register();
        assertThat(LettuceConverterRegistry.getArgConverter(ExpireArgs.class)).isNotNull();
        // Simulate the registry being cleared (e.g. by another test) and verify register() self-heals.
        LettuceConverterRegistry.clear();
        assertThat(LettuceConverterRegistry.getArgConverter(ExpireArgs.class)).isNull();
        LettuceKeyCommandsConverters.register();
        assertThat(LettuceConverterRegistry.getArgConverter(ExpireArgs.class)).isNotNull();
        assertThat(LettuceConverterRegistry.getArgConverter(CopyArgs.class)).isNotNull();
        assertThat(LettuceConverterRegistry.getArgConverter(KeyScanArgs.class)).isNotNull();
    }

    @Test
    void unexpectedExpireTokenFails() {
        ExpireArgs broken = new ExpireArgs() {
            @Override
            public java.util.List<Object> toArgs() {
                return java.util.List.<Object> of("UNKNOWN");
            }
        };
        assertThatThrownBy(() -> LettuceKeyCommandsConverters.toLettuceExpireArgs(broken))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("UNKNOWN");
    }

    private static String[] renderExpireArgs(ExpireArgs quarkus) {
        io.lettuce.core.ExpireArgs lettuce = LettuceKeyCommandsConverters.toLettuceExpireArgs(quarkus);
        return renderToTokens(lettuce::build);
    }

    private static String[] renderCopyArgs(CopyArgs quarkus) {
        io.lettuce.core.CopyArgs lettuce = LettuceKeyCommandsConverters.toLettuceCopyArgs(quarkus);
        return renderToTokens(lettuce::build);
    }

    private static String[] renderKeyScanArgs(KeyScanArgs quarkus) {
        io.lettuce.core.KeyScanArgs lettuce = LettuceKeyCommandsConverters.toLettuceKeyScanArgs(quarkus);
        return renderToTokens(lettuce::build);
    }

    /**
     * Encodes the command args into a RESP byte buffer and extracts the bulk-string values.
     * This avoids the lossy {@code toCommandString()} rendering that base64-encodes or
     * mangles byte-typed values such as the SCAN {@code MATCH} pattern or {@code TYPE}.
     */
    private static String[] renderToTokens(java.util.function.Consumer<CommandArgs<String, String>> builder) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        builder.accept(args);
        io.netty.buffer.ByteBuf buf = io.netty.buffer.ByteBufAllocator.DEFAULT.buffer();
        try {
            args.encode(buf);
            String wire = buf.toString(java.nio.charset.StandardCharsets.UTF_8);
            String[] lines = wire.split("\r\n");
            java.util.List<String> tokens = new java.util.ArrayList<>();
            for (int i = 0; i < lines.length; i++) {
                if (lines[i].startsWith("$") && i + 1 < lines.length) {
                    tokens.add(lines[++i]);
                } else if (lines[i].startsWith(":")) {
                    tokens.add(lines[i].substring(1));
                }
            }
            return tokens.toArray(new String[0]);
        } finally {
            buf.release();
        }
    }
}
