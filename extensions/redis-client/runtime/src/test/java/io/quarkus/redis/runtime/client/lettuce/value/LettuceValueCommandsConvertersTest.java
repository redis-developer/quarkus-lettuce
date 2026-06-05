package io.quarkus.redis.runtime.client.lettuce.value;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.value.GetExArgs;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.runtime.client.lettuce.LettuceConverterRegistry;

/**
 * Unit tests for {@link LettuceValueCommandsConverters}.
 * <p>
 * Each test builds a Quarkus {@link SetArgs}/{@link GetExArgs}, converts it, then renders
 * the Lettuce args to wire format via {@link CommandArgs#build} and asserts the resulting
 * token list matches the Quarkus {@code toArgs()} output.
 */
class LettuceValueCommandsConvertersTest {

    @Test
    void setArgsEx() {
        SetArgs q = new SetArgs().ex(42);
        assertThat(renderSetArgs(q)).containsExactly("EX", "42");
    }

    @Test
    void setArgsExAt() {
        SetArgs q = new SetArgs().exAt(1_700_000_000L);
        assertThat(renderSetArgs(q)).containsExactly("EXAT", "1700000000");
    }

    @Test
    void setArgsPx() {
        SetArgs q = new SetArgs().px(1500);
        assertThat(renderSetArgs(q)).containsExactly("PX", "1500");
    }

    @Test
    void setArgsPxAt() {
        SetArgs q = new SetArgs().pxAt(Instant.ofEpochMilli(1_700_000_000_000L));
        assertThat(renderSetArgs(q)).containsExactly("PXAT", "1700000000000");
    }

    @Test
    void setArgsNx() {
        assertThat(renderSetArgs(new SetArgs().nx())).containsExactly("NX");
    }

    @Test
    void setArgsXx() {
        assertThat(renderSetArgs(new SetArgs().xx())).containsExactly("XX");
    }

    @Test
    void setArgsKeepttl() {
        assertThat(renderSetArgs(new SetArgs().keepttl())).containsExactly("KEEPTTL");
    }

    @Test
    void setArgsGetFlagIsIgnored() {
        // GET is handled via Lettuce's dedicated setGet() method, not via SetArgs
        assertThat(renderSetArgs(new SetArgs().get())).isEmpty();
    }

    @Test
    void setArgsCombinedNxEx() {
        SetArgs q = new SetArgs().nx().ex(Duration.ofSeconds(30));
        assertThat(renderSetArgs(q)).containsExactly("EX", "30", "NX");
    }

    @Test
    void setArgsEmpty() {
        assertThat(renderSetArgs(new SetArgs())).isEmpty();
    }

    @Test
    void getExArgsEx() {
        assertThat(renderGetExArgs(new GetExArgs().ex(42))).containsExactly("EX", "42");
    }

    @Test
    void getExArgsExAt() {
        assertThat(renderGetExArgs(new GetExArgs().exAt(1_700_000_000L)))
                .containsExactly("EXAT", "1700000000");
    }

    @Test
    void getExArgsPx() {
        assertThat(renderGetExArgs(new GetExArgs().px(1500))).containsExactly("PX", "1500");
    }

    @Test
    void getExArgsPxAt() {
        assertThat(renderGetExArgs(new GetExArgs().pxAt(1_700_000_000_000L)))
                .containsExactly("PXAT", "1700000000000");
    }

    @Test
    void getExArgsPersist() {
        assertThat(renderGetExArgs(new GetExArgs().persist())).containsExactly("PERSIST");
    }

    @Test
    void getExArgsEmpty() {
        assertThat(renderGetExArgs(new GetExArgs())).isEmpty();
    }

    @Test
    void registerReinstatesConvertersAfterRegistryCleared() {
        LettuceValueCommandsConverters.register();
        assertThat(LettuceConverterRegistry.getArgConverter(SetArgs.class)).isNotNull();
        // Simulate the registry being cleared (e.g. by another test) and verify register() self-heals.
        LettuceConverterRegistry.clear();
        assertThat(LettuceConverterRegistry.getArgConverter(SetArgs.class)).isNull();
        LettuceValueCommandsConverters.register();
        assertThat(LettuceConverterRegistry.getArgConverter(SetArgs.class)).isNotNull();
        assertThat(LettuceConverterRegistry.getArgConverter(GetExArgs.class)).isNotNull();
    }

    private static String[] renderSetArgs(SetArgs quarkus) {
        io.lettuce.core.SetArgs lettuce = LettuceValueCommandsConverters.toLettuceSetArgs(quarkus);
        return renderToTokens(lettuce::build);
    }

    private static String[] renderGetExArgs(GetExArgs quarkus) {
        io.lettuce.core.GetExArgs lettuce = LettuceValueCommandsConverters.toLettuceGetExArgs(quarkus);
        return renderToTokens(lettuce::build);
    }

    private static String[] renderToTokens(java.util.function.Consumer<CommandArgs<String, String>> builder) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        builder.accept(args);
        // CommandArgs.toCommandString() renders tokens space-separated, unquoted
        String rendered = args.toCommandString();
        if (rendered == null || rendered.isEmpty()) {
            return new String[0];
        }
        return rendered.split(" ");
    }
}
