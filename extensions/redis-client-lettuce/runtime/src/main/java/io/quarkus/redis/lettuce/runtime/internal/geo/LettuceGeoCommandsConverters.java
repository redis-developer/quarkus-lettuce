package io.quarkus.redis.lettuce.runtime.internal.geo;

import java.util.List;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoCoordinates;
import io.lettuce.core.GeoSearch;
import io.lettuce.core.GeoValue;
import io.lettuce.core.protocol.CommandArgs;
import io.quarkus.redis.datasource.codecs.Codec;
import io.quarkus.redis.datasource.geo.GeoAddArgs;
import io.quarkus.redis.datasource.geo.GeoItem;
import io.quarkus.redis.datasource.geo.GeoPosition;
import io.quarkus.redis.datasource.geo.GeoRadiusArgs;
import io.quarkus.redis.datasource.geo.GeoRadiusStoreArgs;
import io.quarkus.redis.datasource.geo.GeoSearchArgs;
import io.quarkus.redis.datasource.geo.GeoSearchStoreArgs;
import io.quarkus.redis.datasource.geo.GeoUnit;
import io.quarkus.redis.lettuce.runtime.internal.ArgReplay;
import io.quarkus.redis.runtime.datasource.Marshaller;

public final class LettuceGeoCommandsConverters {

    private LettuceGeoCommandsConverters() {
        // Utility class
    }

    public static io.lettuce.core.GeoAddArgs toGeoAddArgs(GeoAddArgs quarkus) {
        // Snapshot the tokens now so the XX/NX validation in toArgs() runs at call time, not when Lettuce builds
        // the command.
        List<Object> tokens = quarkus.toArgs();
        return new io.lettuce.core.GeoAddArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(tokens, args);
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <V> GeoValue<byte[]>[] toGeoValues(GeoItem<V>[] items, Marshaller marshaller) {
        GeoValue<byte[]>[] values = new GeoValue[items.length];
        for (int i = 0; i < items.length; i++) {
            GeoItem<V> item = items[i];
            values[i] = GeoValue.just(item.longitude(), item.latitude(), marshaller.encode(item.member()));
        }
        return values;
    }

    public static GeoArgs.Unit toUnit(GeoUnit unit) {
        return switch (unit) {
            case M -> GeoArgs.Unit.m;
            case KM -> GeoArgs.Unit.km;
            case FT -> GeoArgs.Unit.ft;
            case MI -> GeoArgs.Unit.mi;
        };
    }

    public static GeoArgs toGeoArgs(GeoRadiusArgs quarkus) {
        List<Object> tokens = quarkus.toArgs();
        GeoArgs lettuce = new GeoArgs() {
            @Override
            public <K, V> void build(CommandArgs<K, V> args) {
                ArgReplay.replay(tokens, args);
            }
        };
        // Set fields for Lettuce output parsing
        if (quarkus.hasDistance()) {
            lettuce.withDistance();
        }
        if (quarkus.hasCoordinates()) {
            lettuce.withCoordinates();
        }
        if (quarkus.hasHash()) {
            lettuce.withHash();
        }
        return lettuce;
    }

    public static <K> io.lettuce.core.GeoRadiusStoreArgs<byte[]> toGeoRadiusStoreArgs(GeoRadiusStoreArgs<K> quarkus,
            Codec keyCodec, Marshaller marshaller) {
        List<Object> tokens = quarkus.toArgs(keyCodec);
        io.lettuce.core.GeoRadiusStoreArgs<byte[]> lettuce = new io.lettuce.core.GeoRadiusStoreArgs<>() {
            @Override
            public <K1, V1> void build(CommandArgs<K1, V1> args) {
                ArgReplay.replay(tokens, args);
            }
        };
        if (quarkus.getStoreKey() != null) {
            lettuce.withStore(marshaller.encode(quarkus.getStoreKey()));
        }
        if (quarkus.getStoreDistKey() != null) {
            lettuce.withStoreDist(marshaller.encode(quarkus.getStoreDistKey()));
        }
        return lettuce;
    }

    public record GeoSearchParts(GeoSearch.GeoRef<byte[]> reference, GeoSearch.GeoPredicate predicate, GeoArgs args) {
    }

    /**
     * Converts {@link GeoSearchStoreArgs} into the typed parts Lettuce's {@code GEOSEARCHSTORE} expects.
     * <p>
     * Unlike the other converters the tokens are not replayed, because Lettuce builds {@code GEOSEARCH} and
     * {@code GEOSEARCHSTORE} from typed parts (reference, predicate, options). {@code toArgs(codec)} is still
     * invoked so the Quarkus validation (ANY/COUNT, BYRADIUS/BYBOX, FROMMEMBER/FROMLONLAT) stays the single
     * source of truth for both backends.
     */
    public static <V> GeoSearchParts toGeoSearch(GeoSearchStoreArgs<V> quarkus, Codec valueCodec, Marshaller marshaller) {
        requireShape(quarkus.getUnit());
        quarkus.toArgs(valueCodec);
        return toGeoSearch(quarkus.getMember(), quarkus.getLongitude(), quarkus.getLatitude(), quarkus.getRadius(),
                quarkus.getWidth(), quarkus.getHeight(), quarkus.getUnit(), quarkus.getCount(), quarkus.isAny(),
                quarkus.getDirection(), marshaller);
    }

    /**
     * Converts {@link GeoSearchArgs} into the typed parts Lettuce's {@code GEOSEARCH} expects.
     *
     * @see #toGeoSearch(GeoSearchStoreArgs, Codec, Marshaller)
     */
    public static <V> GeoSearchParts toGeoSearch(GeoSearchArgs<V> quarkus, Codec valueCodec, Marshaller marshaller) {
        requireShape(quarkus.getUnit());
        quarkus.toArgs(valueCodec);
        GeoSearchParts parts = toGeoSearch(quarkus.getMember(), quarkus.getLongitude(), quarkus.getLatitude(),
                quarkus.getRadius(), quarkus.getWidth(), quarkus.getHeight(), quarkus.getUnit(), quarkus.getCount(),
                quarkus.isAny(), quarkus.getDirection(), marshaller);
        if (quarkus.hasDistance()) {
            parts.args().withDistance();
        }
        if (quarkus.hasCoordinates()) {
            parts.args().withCoordinates();
        }
        if (quarkus.hasHash()) {
            parts.args().withHash();
        }
        return parts;
    }

    /**
     * {@code toArgs(codec)} dereferences the unit without checking it, so guard the "neither byRadius nor byBox"
     * case here to fail with a clear message instead of a {@link NullPointerException}.
     */
    private static void requireShape(GeoUnit unit) {
        if (unit == null) {
            throw new IllegalArgumentException("Either `byRadius` or `byBox` must be set");
        }
    }

    private static <V> GeoSearchParts toGeoSearch(V member, double longitude, double latitude, double radius, double width,
            double height, GeoUnit unit, long count, boolean any, String direction, Marshaller marshaller) {
        // Validation already ran in toArgs(codec); only the shape decisions mirror it here.
        GeoSearch.GeoRef<byte[]> reference = member != null
                ? GeoSearch.fromMember(marshaller.encode(member))
                : GeoSearch.fromCoordinates(longitude, latitude);

        GeoArgs.Unit lettuceUnit = toUnit(unit);
        GeoSearch.GeoPredicate predicate = radius > 0
                ? GeoSearch.byRadius(radius, lettuceUnit)
                : GeoSearch.byBox(width, height, lettuceUnit);

        GeoArgs args = new GeoArgs();
        if ("ASC".equals(direction)) {
            args.asc();
        } else if ("DESC".equals(direction)) {
            args.desc();
        }
        if (count > 0) {
            args.withCount(count, any);
        }
        return new GeoSearchParts(reference, predicate, args);
    }

    public static GeoPosition toGeoPosition(GeoCoordinates coordinates) {
        if (coordinates == null) {
            return null;
        }
        return GeoPosition.of(coordinates.getX().doubleValue(), coordinates.getY().doubleValue());
    }

}
