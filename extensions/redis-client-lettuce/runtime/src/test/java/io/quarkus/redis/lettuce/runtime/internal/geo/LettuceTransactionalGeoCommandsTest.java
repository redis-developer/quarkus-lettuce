package io.quarkus.redis.lettuce.runtime.internal.geo;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.geo.GeoItem;
import io.quarkus.redis.datasource.geo.GeoPosition;
import io.quarkus.redis.datasource.geo.GeoSearchArgs;
import io.quarkus.redis.datasource.geo.GeoUnit;
import io.quarkus.redis.datasource.geo.GeoValue;
import io.quarkus.redis.datasource.geo.ReactiveTransactionalGeoCommands;
import io.quarkus.redis.datasource.geo.TransactionalGeoCommands;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.lettuce.runtime.internal.CommandsTestBase;

class LettuceTransactionalGeoCommandsTest extends CommandsTestBase {

    RedisDataSource blockingDs;
    ReactiveRedisDataSource reactiveDs;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        blockingDs = blockingDataSource(Duration.ofSeconds(60));
    }

    @Test
    void geoBlocking() {
        TransactionResult result = blockingDs.withTransaction(tx -> {
            TransactionalGeoCommands<String, String> geo = tx.geo(String.class);
            assertThat(geo.getDataSource()).isEqualTo(tx);
            geo.geoadd(key, 10, 10, "1"); // 0 - true
            geo.geoadd(key, GeoItem.of("2", GeoPosition.of(-1, -1))); // 1 - true
            geo.geoadd(key, GeoItem.of("3", -20, -20)); // 2 - true
            geo.geodist(key, "1", "3", GeoUnit.KM); // 3 - some number
            geo.geopos(key, "2", "3", "4"); // 4 - list of position
            geo.geosearch(key, new GeoSearchArgs<String>().withDistance().ascending().byRadius(10000, GeoUnit.KM)); // 5 - list of geo value
        });
        assertThat(result.size()).isEqualTo(6);
        assertThat(result.discarded()).isFalse();
        assertThat((Boolean) result.get(0)).isTrue();
        assertThat((Boolean) result.get(1)).isTrue();
        assertThat((Boolean) result.get(2)).isTrue();
        assertThat((Double) result.get(3)).isPositive();
        List<GeoPosition> list = result.get(4);
        assertThat(list).hasSize(3);
        assertThat(list.get(2)).isNull();
        List<GeoValue<String>> values = result.get(5);
        assertThat(values).hasSize(3);
    }

    @Test
    void geoReactive() {
        TransactionResult result = reactiveDs.withTransaction(tx -> {
            ReactiveTransactionalGeoCommands<String, String> geo = tx.geo(String.class);
            return geo.geoadd(key, 10, 10, "1") // 0 - true
                    .chain(() -> geo.geoadd(key, GeoItem.of("2", GeoPosition.of(-1, -1)))) // 1 - true
                    .chain(() -> geo.geoadd(key, GeoItem.of("3", -20, -20))) // 2 - true
                    .chain(() -> geo.geodist(key, "1", "3", GeoUnit.KM)) // 3 - some number
                    .chain(() -> geo.geopos(key, "2", "3", "4")) // 4 - list of position
                    .chain(() -> geo.geosearch(key,
                            new GeoSearchArgs<String>().withDistance().ascending().byRadius(10000, GeoUnit.KM))); // 5 - list of geo value
        }).await().atMost(Duration.ofSeconds(5));
        assertThat(result.size()).isEqualTo(6);
        assertThat(result.discarded()).isFalse();
        assertThat((Boolean) result.get(0)).isTrue();
        assertThat((Boolean) result.get(1)).isTrue();
        assertThat((Boolean) result.get(2)).isTrue();
        assertThat((Double) result.get(3)).isPositive();
        List<GeoPosition> list = result.get(4);
        assertThat(list).hasSize(3);
        List<GeoValue<String>> values = result.get(5);
        assertThat(values).hasSize(3);
    }

}
