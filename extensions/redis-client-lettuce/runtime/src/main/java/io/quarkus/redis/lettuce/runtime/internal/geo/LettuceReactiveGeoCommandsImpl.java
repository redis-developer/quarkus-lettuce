package io.quarkus.redis.lettuce.runtime.internal.geo;

import static io.quarkus.redis.runtime.datasource.Validation.notNullOrEmpty;
import static io.quarkus.redis.runtime.datasource.Validation.positive;
import static io.quarkus.redis.runtime.datasource.Validation.validateLatitude;
import static io.quarkus.redis.runtime.datasource.Validation.validateLongitude;
import static io.smallrye.mutiny.helpers.ParameterValidation.doesNotContainNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoCoordinates;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.Value;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.codecs.Codec;
import io.quarkus.redis.datasource.codecs.Codecs;
import io.quarkus.redis.datasource.geo.GeoAddArgs;
import io.quarkus.redis.datasource.geo.GeoItem;
import io.quarkus.redis.datasource.geo.GeoPosition;
import io.quarkus.redis.datasource.geo.GeoRadiusArgs;
import io.quarkus.redis.datasource.geo.GeoRadiusStoreArgs;
import io.quarkus.redis.datasource.geo.GeoSearchArgs;
import io.quarkus.redis.datasource.geo.GeoSearchStoreArgs;
import io.quarkus.redis.datasource.geo.GeoUnit;
import io.quarkus.redis.datasource.geo.GeoValue;
import io.quarkus.redis.datasource.geo.ReactiveGeoCommands;
import io.quarkus.redis.lettuce.runtime.internal.AbstractLettuceCommands;
import io.quarkus.redis.lettuce.runtime.internal.LettuceCommand;
import io.quarkus.redis.runtime.datasource.Marshaller;
import io.smallrye.mutiny.Uni;

/**
 * Lettuce-backed implementation of {@link ReactiveGeoCommands}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class LettuceReactiveGeoCommandsImpl<K, V> extends AbstractLettuceCommands<K, V>
        implements ReactiveGeoCommands<K, V> {

    static final GeoAddArgs DEFAULT_INSTANCE = new GeoAddArgs();

    private final ReactiveRedisDataSource dataSource;
    private final Codec keyCodec;
    private final Codec valueCodec;

    public LettuceReactiveGeoCommandsImpl(ReactiveRedisDataSource dataSource,
            StatefulRedisConnection<byte[], byte[]> connection, Type keyType, Type valueType) {
        super(connection, keyType, valueType, new Marshaller(keyType, valueType));
        this.dataSource = dataSource;
        this.keyCodec = Codecs.getDefaultCodecFor(keyType);
        this.valueCodec = Codecs.getDefaultCodecFor(valueType);
    }

    @Override
    public ReactiveRedisDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Uni<Boolean> geoadd(K key, double longitude, double latitude, V member) {
        return geoadd(key, longitude, latitude, member, DEFAULT_INSTANCE);
    }

    @Override
    public Uni<Boolean> geoadd(K key, GeoPosition position, V member) {
        nonNull(position, "position");
        return geoadd(key, position.longitude, position.latitude, member);
    }

    @Override
    public Uni<Boolean> geoadd(K key, GeoItem<V> item) {
        nonNull(item, "item");
        return geoadd(key, item.longitude(), item.latitude(), item.member);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> geoadd(K key, GeoItem<V>... items) {
        return _geoadd(key, items).toUni();
    }

    @SafeVarargs
    final LettuceCommand<Long, Integer> _geoadd(K key, GeoItem<V>... items) {
        nonNull(key, "key");
        notNullOrEmpty(items, "items");
        doesNotContainNull(items, "items");
        return LettuceCommand.of(
                () -> async.geoadd(marshaller.encode(key), LettuceGeoCommandsConverters.toGeoValues(items, marshaller)),
                AbstractLettuceCommands::toInteger);
    }

    @Override
    public Uni<Boolean> geoadd(K key, double longitude, double latitude, V member, GeoAddArgs args) {
        return _geoadd(key, longitude, latitude, member, args).toUni();
    }

    LettuceCommand<Long, Boolean> _geoadd(K key, double longitude, double latitude, V member, GeoAddArgs args) {
        nonNull(key, "key");
        nonNull(member, "member");
        nonNull(args, "args");
        validateLongitude(longitude);
        validateLatitude(latitude);
        io.lettuce.core.GeoAddArgs lettuceArgs = LettuceGeoCommandsConverters.toGeoAddArgs(args);
        return LettuceCommand.of(() -> async.geoadd(marshaller.encode(key), longitude, latitude, marshaller.encode(member),
                lettuceArgs), AbstractLettuceCommands::asBoolean);
    }

    @Override
    public Uni<Boolean> geoadd(K key, GeoItem<V> item, GeoAddArgs args) {
        nonNull(item, "item");
        return geoadd(key, item.longitude(), item.latitude(), item.member(), args);
    }

    @SafeVarargs
    @Override
    public final Uni<Integer> geoadd(K key, GeoAddArgs args, GeoItem<V>... items) {
        return _geoadd(key, args, items).toUni();
    }

    @SafeVarargs
    final LettuceCommand<Long, Integer> _geoadd(K key, GeoAddArgs args, GeoItem<V>... items) {
        nonNull(key, "key");
        notNullOrEmpty(items, "items");
        doesNotContainNull(items, "items");
        nonNull(args, "args");
        io.lettuce.core.GeoAddArgs lettuceArgs = LettuceGeoCommandsConverters.toGeoAddArgs(args);
        return LettuceCommand.of(
                () -> async.geoadd(marshaller.encode(key), lettuceArgs,
                        LettuceGeoCommandsConverters.toGeoValues(items, marshaller)),
                AbstractLettuceCommands::toInteger);
    }

    @Override
    public Uni<Double> geodist(K key, V from, V to, GeoUnit unit) {
        return _geodist(key, from, to, unit).toUni();
    }

    LettuceCommand<Double, Double> _geodist(K key, V from, V to, GeoUnit unit) {
        nonNull(key, "key");
        nonNull(from, "from");
        nonNull(to, "to");
        nonNull(unit, "unit");
        return LettuceCommand.of(() -> async.geodist(marshaller.encode(key), marshaller.encode(from), marshaller.encode(to),
                LettuceGeoCommandsConverters.toUnit(unit)));
    }

    @SafeVarargs
    @Override
    public final Uni<List<String>> geohash(K key, V... members) {
        return _geohash(key, members).toUni();
    }

    @SafeVarargs
    final LettuceCommand<List<Value<String>>, List<String>> _geohash(K key, V... members) {
        nonNull(key, "key");
        notNullOrEmpty(members, "members");
        doesNotContainNull(members, "members");
        return LettuceCommand.of(() -> async.geohash(marshaller.encode(key), marshaller.encodeAsArray(members)),
                LettuceReactiveGeoCommandsImpl::decodeHashes);
    }

    @SafeVarargs
    @Override
    public final Uni<List<GeoPosition>> geopos(K key, V... members) {
        return _geopos(key, members).toUni();
    }

    @SafeVarargs
    final LettuceCommand<List<GeoCoordinates>, List<GeoPosition>> _geopos(K key, V... members) {
        nonNull(key, "key");
        notNullOrEmpty(members, "members");
        doesNotContainNull(members, "members");
        return LettuceCommand.of(() -> async.geopos(marshaller.encode(key), marshaller.encodeAsArray(members)),
                LettuceReactiveGeoCommandsImpl::decodePositions);
    }

    @Override
    public Uni<Set<V>> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit) {
        return _georadius(key, longitude, latitude, radius, unit).toUni();
    }

    LettuceCommand<Set<byte[]>, Set<V>> _georadius(K key, double longitude, double latitude, double radius, GeoUnit unit) {
        nonNull(key, "key");
        positive(radius, "radius");
        validateLongitude(longitude);
        validateLatitude(latitude);
        nonNull(unit, "unit");
        return LettuceCommand.of(() -> async.georadius(marshaller.encode(key), longitude, latitude, radius,
                LettuceGeoCommandsConverters.toUnit(unit)), this::decodeSetOfValue);
    }

    @Override
    public Uni<Set<V>> georadius(K key, GeoPosition position, double radius, GeoUnit unit) {
        nonNull(position, "position");
        return georadius(key, position.longitude, position.latitude, radius, unit);
    }

    @Override
    public Uni<List<GeoValue<V>>> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit,
            GeoRadiusArgs geoArgs) {
        return _georadius(key, longitude, latitude, radius, unit, geoArgs).toUni();
    }

    LettuceCommand<List<GeoWithin<byte[]>>, List<GeoValue<V>>> _georadius(K key, double longitude, double latitude,
            double radius, GeoUnit unit, GeoRadiusArgs geoArgs) {
        nonNull(key, "key");
        validateLongitude(longitude);
        validateLatitude(latitude);
        positive(radius, "radius");
        nonNull(unit, "unit");
        nonNull(geoArgs, "geoArgs");
        GeoArgs lettuceArgs = LettuceGeoCommandsConverters.toGeoArgs(geoArgs);
        return LettuceCommand.of(() -> async.georadius(marshaller.encode(key), longitude, latitude, radius,
                LettuceGeoCommandsConverters.toUnit(unit), lettuceArgs), this::decodeGeoValues);
    }

    @Override
    public Uni<List<GeoValue<V>>> georadius(K key, GeoPosition position, double radius, GeoUnit unit, GeoRadiusArgs geoArgs) {
        nonNull(position, "position");
        return georadius(key, position.longitude, position.latitude, radius, unit, geoArgs);
    }

    @Override
    public Uni<Long> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit,
            GeoRadiusStoreArgs<K> geoArgs) {
        return _georadius(key, longitude, latitude, radius, unit, geoArgs).toUni();
    }

    LettuceCommand<Long, Long> _georadius(K key, double longitude, double latitude, double radius, GeoUnit unit,
            GeoRadiusStoreArgs<K> geoArgs) {
        nonNull(key, "key");
        validateLongitude(longitude);
        validateLatitude(latitude);
        positive(radius, "radius");
        nonNull(unit, "unit");
        nonNull(geoArgs, "geoArgs");
        CommandArgs<byte[], byte[]> args = LettuceGeoCommandsConverters.toGeoRadiusStoreCommandArgs(marshaller.encode(key),
                longitude, latitude, radius, unit, geoArgs, keyCodec);
        return LettuceCommand.of(() -> async.dispatch(CommandType.GEORADIUS, new IntegerOutput<>(ByteArrayCodec.INSTANCE),
                args));
    }

    @Override
    public Uni<Long> georadius(K key, GeoPosition position, double radius, GeoUnit unit, GeoRadiusStoreArgs<K> geoArgs) {
        nonNull(position, "position");
        return georadius(key, position.longitude, position.latitude, radius, unit, geoArgs);
    }

    @Override
    public Uni<Set<V>> georadiusbymember(K key, V member, double distance, GeoUnit unit) {
        return _georadiusbymember(key, member, distance, unit).toUni();
    }

    LettuceCommand<Set<byte[]>, Set<V>> _georadiusbymember(K key, V member, double distance, GeoUnit unit) {
        nonNull(key, "key");
        nonNull(member, "member");
        positive(distance, "distance");
        nonNull(unit, "unit");
        return LettuceCommand.of(() -> async.georadiusbymember(marshaller.encode(key), marshaller.encode(member), distance,
                LettuceGeoCommandsConverters.toUnit(unit)), this::decodeSetOfValue);
    }

    @Override
    public Uni<List<GeoValue<V>>> georadiusbymember(K key, V member, double distance, GeoUnit unit, GeoRadiusArgs geoArgs) {
        return _georadiusbymember(key, member, distance, unit, geoArgs).toUni();
    }

    LettuceCommand<List<GeoWithin<byte[]>>, List<GeoValue<V>>> _georadiusbymember(K key, V member, double distance,
            GeoUnit unit, GeoRadiusArgs geoArgs) {
        nonNull(key, "key");
        nonNull(member, "member");
        positive(distance, "distance");
        nonNull(unit, "unit");
        nonNull(geoArgs, "geoArgs");
        GeoArgs lettuceArgs = LettuceGeoCommandsConverters.toGeoArgs(geoArgs);
        return LettuceCommand.of(() -> async.georadiusbymember(marshaller.encode(key), marshaller.encode(member), distance,
                LettuceGeoCommandsConverters.toUnit(unit), lettuceArgs), this::decodeGeoValues);
    }

    @Override
    public Uni<Long> georadiusbymember(K key, V member, double distance, GeoUnit unit, GeoRadiusStoreArgs<K> geoArgs) {
        return _georadiusbymember(key, member, distance, unit, geoArgs).toUni();
    }

    LettuceCommand<Long, Long> _georadiusbymember(K key, V member, double distance, GeoUnit unit,
            GeoRadiusStoreArgs<K> geoArgs) {
        nonNull(key, "key");
        nonNull(member, "member");
        positive(distance, "distance");
        nonNull(unit, "unit");
        nonNull(geoArgs, "geoArgs");
        CommandArgs<byte[], byte[]> args = LettuceGeoCommandsConverters.toGeoRadiusByMemberStoreCommandArgs(
                marshaller.encode(key), marshaller.encode(member), distance, unit, geoArgs, keyCodec);
        return LettuceCommand.of(() -> async.dispatch(CommandType.GEORADIUSBYMEMBER,
                new IntegerOutput<>(ByteArrayCodec.INSTANCE), args));
    }

    @Override
    public Uni<List<GeoValue<V>>> geosearch(K key, GeoSearchArgs<V> args) {
        return _geosearch(key, args).toUni();
    }

    LettuceCommand<List<GeoWithin<byte[]>>, List<GeoValue<V>>> _geosearch(K key, GeoSearchArgs<V> geoArgs) {
        nonNull(key, "key");
        nonNull(geoArgs, "geoArgs");
        CommandArgs<byte[], byte[]> args = LettuceGeoCommandsConverters.toGeoSearchCommandArgs(marshaller.encode(key),
                geoArgs, valueCodec);
        return LettuceCommand.of(
                () -> async.dispatch(CommandType.GEOSEARCH, LettuceGeoCommandsConverters.toGeoWithinOutput(geoArgs), args),
                this::decodeGeoValues);
    }

    @Override
    public Uni<Long> geosearchstore(K destination, K key, GeoSearchStoreArgs<V> args, boolean storeDist) {
        return _geosearchstore(destination, key, args, storeDist).toUni();
    }

    LettuceCommand<Long, Long> _geosearchstore(K destination, K key, GeoSearchStoreArgs<V> geoArgs, boolean storeDist) {
        nonNull(destination, "destination");
        nonNull(key, "key");
        nonNull(geoArgs, "geoArgs");
        CommandArgs<byte[], byte[]> args = LettuceGeoCommandsConverters.toGeoSearchStoreCommandArgs(
                marshaller.encode(destination), marshaller.encode(key), geoArgs, valueCodec, storeDist);
        return LettuceCommand.of(() -> async.dispatch(CommandType.GEOSEARCHSTORE,
                new IntegerOutput<>(ByteArrayCodec.INSTANCE), args));
    }

    private static List<String> decodeHashes(List<Value<String>> hashes) {
        if (hashes == null || hashes.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> decoded = new ArrayList<>();
        for (Value<String> hash : hashes) {
            decoded.add(hash.hasValue() ? hash.getValue() : null);
        }
        return decoded;
    }

    private static List<GeoPosition> decodePositions(List<GeoCoordinates> coordinates) {
        if (coordinates == null || coordinates.isEmpty()) {
            return Collections.emptyList();
        }
        List<GeoPosition> decoded = new ArrayList<>();
        for (GeoCoordinates coordinate : coordinates) {
            decoded.add(LettuceGeoCommandsConverters.toGeoPosition(coordinate));
        }
        return decoded;
    }

    private Set<V> decodeSetOfValue(Set<byte[]> members) {
        if (members == null) {
            return new LinkedHashSet<>();
        }
        Set<V> decoded = new LinkedHashSet<>(members.size());
        for (byte[] member : members) {
            decoded.add(decodeV(member));
        }
        return decoded;
    }

    private List<GeoValue<V>> decodeGeoValues(List<GeoWithin<byte[]>> values) {
        List<GeoValue<V>> decoded = new ArrayList<>();
        for (GeoWithin<byte[]> within : orEmpty(values)) {
            decoded.add(decodeGeoValue(within));
        }
        return decoded;
    }

    private GeoValue<V> decodeGeoValue(GeoWithin<byte[]> within) {
        V member = decodeV(within.getMember());
        OptionalDouble distance = within.getDistance() == null ? OptionalDouble.empty()
                : OptionalDouble.of(within.getDistance());
        OptionalLong geohash = within.getGeohash() == null ? OptionalLong.empty() : OptionalLong.of(within.getGeohash());
        GeoCoordinates coordinates = within.getCoordinates();
        OptionalDouble longitude = coordinates == null ? OptionalDouble.empty()
                : OptionalDouble.of(coordinates.getX().doubleValue());
        OptionalDouble latitude = coordinates == null ? OptionalDouble.empty()
                : OptionalDouble.of(coordinates.getY().doubleValue());
        return new GeoValue<>(member, distance, geohash, longitude, latitude);
    }

}
