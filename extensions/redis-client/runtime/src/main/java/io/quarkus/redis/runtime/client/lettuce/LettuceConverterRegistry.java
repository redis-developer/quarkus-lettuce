package io.quarkus.redis.runtime.client.lettuce;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Central registry for type converters between Quarkus Redis types and Lettuce types.
 * <p>
 * Converters are registered by source type and looked up at command execution time.
 * Each command story (Value Commands, Key Commands, etc.) registers its own converters
 * when loaded.
 * <p>
 * Converters are stateless functions: {@code Function<QuarkusType, LettuceType>}.
 *
 * <h3>Usage</h3>
 *
 * <pre>{@code
 * // Register (typically at startup)
 * LettuceConverterRegistry.registerArgConverter(
 *     io.quarkus.redis.datasource.value.SetArgs.class,
 *     quarkusSetArgs -> { ... return lettuceSetArgs; }
 * );
 *
 * // Lookup
 * Function<Object, Object> converter = LettuceConverterRegistry.getArgConverter(
 *     io.quarkus.redis.datasource.value.SetArgs.class
 * );
 * }</pre>
 */
public final class LettuceConverterRegistry {

    /**
     * Converters for command arguments: Quarkus arg type → Lettuce arg type.
     */
    private static final Map<Class<?>, Function<Object, Object>> ARG_CONVERTERS = new ConcurrentHashMap<>();

    /**
     * Converters for command results: Lettuce result type → Quarkus result type.
     */
    private static final Map<Class<?>, Function<Object, Object>> RESULT_CONVERTERS = new ConcurrentHashMap<>();

    private LettuceConverterRegistry() {
        // Utility class
    }

    /**
     * Register a converter for a Quarkus command argument type.
     *
     * @param <S> the Quarkus source type
     * @param <T> the Lettuce target type
     * @param sourceType the Quarkus argument class
     * @param converter the conversion function
     */
    @SuppressWarnings("unchecked")
    public static <S, T> void registerArgConverter(Class<S> sourceType, Function<S, T> converter) {
        ARG_CONVERTERS.put(sourceType, (Function<Object, Object>) converter);
    }

    /**
     * Register a converter for a Lettuce result type.
     *
     * @param <S> the Lettuce source type
     * @param <T> the Quarkus target type
     * @param sourceType the Lettuce result class
     * @param converter the conversion function
     */
    @SuppressWarnings("unchecked")
    public static <S, T> void registerResultConverter(Class<S> sourceType, Function<S, T> converter) {
        RESULT_CONVERTERS.put(sourceType, (Function<Object, Object>) converter);
    }

    /**
     * Look up an argument converter by Quarkus source type.
     *
     * @param sourceType the Quarkus argument class
     * @return the converter, or {@code null} if none is registered
     */
    public static Function<Object, Object> getArgConverter(Class<?> sourceType) {
        return ARG_CONVERTERS.get(sourceType);
    }

    /**
     * Look up a result converter by Lettuce source type.
     *
     * @param sourceType the Lettuce result class
     * @return the converter, or {@code null} if none is registered
     */
    public static Function<Object, Object> getResultConverter(Class<?> sourceType) {
        return RESULT_CONVERTERS.get(sourceType);
    }

    /**
     * Convert a Quarkus argument to its Lettuce equivalent.
     *
     * @param <T> the expected Lettuce type
     * @param arg the Quarkus argument
     * @return the converted Lettuce argument
     * @throws IllegalArgumentException if no converter is registered for the type
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertArg(Object arg) {
        if (arg == null) {
            return null;
        }
        Function<Object, Object> converter = ARG_CONVERTERS.get(arg.getClass());
        if (converter == null) {
            throw new IllegalArgumentException(
                    "No Lettuce converter registered for argument type: " + arg.getClass().getName());
        }
        return (T) converter.apply(arg);
    }

    /**
     * Convert a Lettuce result to its Quarkus equivalent.
     *
     * @param <T> the expected Quarkus type
     * @param result the Lettuce result
     * @return the converted Quarkus result
     * @throws IllegalArgumentException if no converter is registered for the type
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertResult(Object result) {
        if (result == null) {
            return null;
        }
        Function<Object, Object> converter = RESULT_CONVERTERS.get(result.getClass());
        if (converter == null) {
            throw new IllegalArgumentException(
                    "No Lettuce converter registered for result type: " + result.getClass().getName());
        }
        return (T) converter.apply(result);
    }

    /**
     * Clear all registered converters. Intended for testing only.
     */
    public static void clear() {
        ARG_CONVERTERS.clear();
        RESULT_CONVERTERS.clear();
    }
}
