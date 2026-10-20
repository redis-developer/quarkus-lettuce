package io.quarkus.redis.lettuce.runtime.internal.stream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.lettuce.core.Range;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.RedisCommandExtraArguments;
import io.quarkus.redis.datasource.stream.StreamRange;
import io.quarkus.redis.datasource.stream.XAddArgs;
import io.quarkus.redis.datasource.stream.XClaimArgs;
import io.quarkus.redis.datasource.stream.XGroupCreateArgs;
import io.quarkus.redis.datasource.stream.XGroupSetIdArgs;
import io.quarkus.redis.datasource.stream.XPendingArgs;
import io.quarkus.redis.datasource.stream.XReadArgs;
import io.quarkus.redis.datasource.stream.XReadGroupArgs;
import io.quarkus.redis.datasource.stream.XTrimArgs;
import io.quarkus.redis.lettuce.runtime.internal.ArgReplay;
import io.quarkus.redis.lettuce.runtime.internal.BlockingReads;

public final class LettuceStreamCommandsConverters {

    private LettuceStreamCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.XAddArgs toXAddArgs(XAddArgs quarkus) {
        List<Object> tokens = quarkus.toArgs();
        return new io.lettuce.core.XAddArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(tokens, args);
            }
        };
    }

    public static io.lettuce.core.XClaimArgs toXClaimArgs(XClaimArgs quarkus, Duration minIdleTime) {
        List<Object> tokens = quarkus.toArgs();
        io.lettuce.core.XClaimArgs lettuce = new io.lettuce.core.XClaimArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(tokens, args);
            }
        };
        return lettuce.minIdleTime(minIdleTime);
    }

    public static io.lettuce.core.XGroupCreateArgs toXGroupCreateArgs(XGroupCreateArgs quarkus) {
        List<Object> tokens = quarkus.toArgs();
        return new io.lettuce.core.XGroupCreateArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(tokens, args);
            }
        };
    }

    public static CommandArgs<byte[], byte[]> toXGroupSetIdCommandArgs(byte[] key, byte[] group, String from,
            XGroupSetIdArgs quarkus) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE)
                .add("SETID").addKey(key).addKey(group).add(from);
        ArgReplay.replay(quarkus.toArgs(), args);
        return args;
    }

    public static boolean blocks(RedisCommandExtraArguments args) {
        return args != null && args.toArgs().contains("BLOCK");
    }

    public static io.lettuce.core.XReadArgs toXReadArgs(XReadArgs quarkus) {
        List<Object> tokens = quarkus.toArgs();
        boolean blocking = tokens.contains("BLOCK");
        return new io.lettuce.core.XReadArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(tokens, args);
                if (blocking) {
                    BlockingReads.mark(args);
                }
            }
        };
    }

    public static io.lettuce.core.XReadArgs toXReadArgs(XReadGroupArgs quarkus) {
        List<Object> tokens = quarkus.toArgs();
        boolean blocking = tokens.contains("BLOCK");
        return new io.lettuce.core.XReadArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(tokens, args);
                if (blocking) {
                    BlockingReads.mark(args);
                }
            }
        };
    }

    public static io.lettuce.core.XTrimArgs toXTrimArgs(XTrimArgs quarkus) {
        List<Object> tokens = quarkus.toArgs();
        return new io.lettuce.core.XTrimArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(tokens, args);
            }
        };
    }

    public static Range<String> toRange(StreamRange range) {
        List<Object> tokens = range.toArgs();
        return Range.from(lowerBoundary(tokens.get(0)), upperBoundary(tokens.get(1)));
    }

    public static Range<String> toReverseRange(StreamRange range) {
        List<Object> tokens = range.toArgs();
        return Range.from(lowerBoundary(tokens.get(1)), upperBoundary(tokens.get(0)));
    }

    private static Range.Boundary<String> lowerBoundary(Object token) {
        String bound = token.toString();
        return "-".equals(bound) ? Range.Boundary.unbounded() : Range.Boundary.including(bound);
    }

    private static Range.Boundary<String> upperBoundary(Object token) {
        String bound = token.toString();
        return "+".equals(bound) ? Range.Boundary.unbounded() : Range.Boundary.including(bound);
    }

    public static io.lettuce.core.XPendingArgs<byte[]> toXPendingArgs(byte[] group, StreamRange range, int count,
            XPendingArgs args) {
        List<Object> tokens = new ArrayList<>();
        if (args != null && args.idle() != null) {
            tokens.add("IDLE");
            tokens.add(args.idle().toMillis());
        }
        tokens.addAll(range.toArgs());
        tokens.add(count);
        if (args != null) {
            tokens.addAll(args.toArgs());
        }
        return new io.lettuce.core.XPendingArgs<>() {
            @Override
            public <K, V> void build(CommandArgs<K, V> commandArgs) {
                commandArgs.addKey((K) group);
                ArgReplay.replay(tokens, commandArgs);
            }
        };
    }

}
