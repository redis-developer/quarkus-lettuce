package io.quarkus.redis.lettuce.runtime.internal.timeseries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.timeseries.AddArgs;
import io.quarkus.redis.datasource.timeseries.Aggregation;
import io.quarkus.redis.datasource.timeseries.AlterArgs;
import io.quarkus.redis.datasource.timeseries.CreateArgs;
import io.quarkus.redis.datasource.timeseries.DuplicatePolicy;
import io.quarkus.redis.datasource.timeseries.Filter;
import io.quarkus.redis.datasource.timeseries.IncrementArgs;
import io.quarkus.redis.datasource.timeseries.MGetArgs;
import io.quarkus.redis.datasource.timeseries.MRangeArgs;
import io.quarkus.redis.datasource.timeseries.RangeArgs;
import io.quarkus.redis.datasource.timeseries.ReactiveTimeSeriesCommands;
import io.quarkus.redis.datasource.timeseries.Reducer;
import io.quarkus.redis.datasource.timeseries.Sample;
import io.quarkus.redis.datasource.timeseries.SeriesSample;
import io.quarkus.redis.datasource.timeseries.TimeSeriesCommands;
import io.quarkus.redis.datasource.timeseries.TimeSeriesRange;
import io.quarkus.redis.lettuce.runtime.internal.CommandsTestBase;
import io.quarkus.redis.lettuce.runtime.internal.Person;
import tools.jackson.core.type.TypeReference;

@SuppressWarnings("unchecked")
class LettuceTimeSeriesCommandsTest extends CommandsTestBase {

    ReactiveRedisDataSource reactiveDs;
    RedisDataSource blockingDs;
    ReactiveTimeSeriesCommands<String> reactiveTs;
    TimeSeriesCommands<String> blockingTs;

    @BeforeEach
    void initialize() {
        reactiveDs = reactiveDataSource();
        blockingDs = blockingDataSource();
        reactiveTs = reactiveDs.timeseries(String.class);
        blockingTs = blockingDs.timeseries(String.class);
    }

    @Test
    void getDataSource() {
        assertThat(reactiveDs).isEqualTo(reactiveTs.getDataSource());
        assertThat(blockingDs).isEqualTo(blockingTs.getDataSource());
    }

    @Test
    void testCreationAndAddingDataPoint() throws InterruptedException {
        blockingTs.tsCreate(key);
        blockingTs.tsCreate("sensor", new CreateArgs().setRetention(Duration.of(2678400000L, ChronoUnit.MILLIS)));

        blockingTs.tsAlter("sensor", new AlterArgs().chunkSize(1024));

        blockingTs.tsAdd(key, 1626434637914L, 26);
        blockingTs.tsAdd(key, 27);

        Thread.sleep(1); // Do make sure we have a different timestamp
        blockingTs.tsMAdd(SeriesSample.from(key, 51), SeriesSample.from(key, 1626434637915L, 52),
                SeriesSample.from("sensor", 22));

        var list = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(4);

        list = blockingTs.tsRange("sensor", TimeSeriesRange.fromTimestampToLatest(0));
        assertThat(list).hasSize(1);
    }

    @Test
    void testAdd() throws InterruptedException {
        blockingTs.tsAdd(key, 25.0);
        Thread.sleep(10); // Make sure the timestamp is different
        blockingTs.tsAdd(key, 30.5, new AddArgs().label("foo", "bar"));
        var list = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(2);
    }

    @Test
    void testCreationWhileAdding() {
        long timestamp = System.currentTimeMillis() - 1000;
        blockingTs.tsAdd(key, timestamp, 26, new AddArgs().compressed().label("foo", "bar")
                .setRetention(Duration.ofDays(1)).chunkSize(1024));
        blockingTs.tsAdd(key, 27);

        var list = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(2);

        blockingTs.tsAdd(key, timestamp, 25, new AddArgs().onDuplicate(DuplicatePolicy.LAST));
        list = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(2);
        assertThat(list.stream().map(s -> s.value).collect(Collectors.toList())).containsExactlyInAnyOrder(27.0, 25.0);
    }

    @Test
    void testIncrAndDecr() {
        blockingTs.tsCreate(key);
        blockingTs.tsAdd(key, 10, 26);
        blockingTs.tsAdd(key, 20, 30);

        blockingTs.tsIncrBy(key, 12);

        assertThat(blockingTs.tsGet(key).value).isEqualTo(30 + 12);
        blockingTs.tsIncrBy(key, 12, new IncrementArgs().label("foo", "bar"));
        assertThat(blockingTs.tsGet(key).value).isEqualTo(30 + 12 + 12);

        blockingTs.tsDecrBy(key, 2);
        assertThat(blockingTs.tsGet(key).value).isEqualTo(30 + 12 + 12 - 2);
        blockingTs.tsDecrBy(key, 2, new IncrementArgs().uncompressed());
        assertThat(blockingTs.tsGet(key).value).isEqualTo(30 + 12 + 12 - 2 - 2);
    }

    @Test
    void testDeletion() {
        blockingTs.tsCreate(key);
        blockingTs.tsAdd(key, 500, 50);
        blockingTs.tsAdd(key, 501, 51);
        blockingTs.tsAdd(key, 1000, 26);
        blockingTs.tsAdd(key, 1001, 27);
        blockingTs.tsAdd(key, 2000, 28);
        blockingTs.tsAdd(key, 2001, 29);

        var list = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(6);

        blockingTs.tsDel(key, 1000, 1999);
        list = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(4);

        blockingTs.tsDel(key, 501, 501);
        list = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(3);
    }

    @Test
    void testAggregation() {
        blockingTs.tsCreate(key);
        blockingTs.tsCreate("cc");
        blockingTs.tsCreate("avg");

        blockingTs.tsCreateRule(key, "cc", Aggregation.COUNT, Duration.of(1, ChronoUnit.SECONDS));
        blockingTs.tsCreateRule(key, "avg", Aggregation.AVG, Duration.of(10, ChronoUnit.MILLIS));

        blockingTs.tsAdd(key, 500, 50);
        blockingTs.tsAdd(key, 2003, 42);
        blockingTs.tsAdd(key, 501, 51);
        blockingTs.tsAdd(key, 1000, 26);
        blockingTs.tsAdd(key, 1001, 27);
        blockingTs.tsAdd(key, 2000, 28);
        blockingTs.tsAdd(key, 2001, 29);
        blockingTs.tsAdd(key, 1002, 1002339488832.23477);
        blockingTs.tsAdd(key, 1003, 45.56778);
        blockingTs.tsAdd(key, 2004, 45);
        blockingTs.tsAdd(key, 3000, 45);

        var counts = blockingTs.tsRange("cc", TimeSeriesRange.fromEarliestToTimestamp(5000));
        assertThat(counts).hasSize(3);

        var avg = blockingTs.tsRange("avg", TimeSeriesRange.fromEarliestToTimestamp(5000));
        assertThat(avg).hasSize(3);

        blockingTs.tsDeleteRule(key, "cc");
    }

    @Test
    void testFiltering() {
        blockingTs.tsCreate(key, new CreateArgs()
                .label("area", 1)
                .label("common", "1")
                .label("foo", "bar"));
        blockingTs.tsCreate("sensor", new CreateArgs()
                .label("common", "1")
                .label("foo", "baz"));

        blockingTs.tsAdd(key, 500, 50, new AddArgs().label("area", 51));
        blockingTs.tsAdd("sensor", 2003, 42, new AddArgs().label("area", 52));
        blockingTs.tsAdd("sensor", 501, 51, new AddArgs().label("area", 51));
        blockingTs.tsAdd(key, 1000, 26, new AddArgs().label("area", 51));
        blockingTs.tsAdd(key, 1001, 27, new AddArgs().label("area", 32));
        blockingTs.tsAdd(key, 2000, 28, new AddArgs().label("area", 51).label("name", "h1"));
        blockingTs.tsAdd("sensor", 2001, 29, new AddArgs().label("area", 52));
        blockingTs.tsAdd(key, 1002, 1002339488832.23477, new AddArgs().label("area", 53));
        blockingTs.tsAdd("sensor", 1003, 45.56778, new AddArgs().label("area", 12));
        blockingTs.tsAdd("sensor", 2004, 45, new AddArgs().label("area", 12));
        blockingTs.tsAdd(key, 3000, 45, new AddArgs().label("area", 12));

        var map = blockingTs.tsMRange(TimeSeriesRange.fromTimestamps(10, 10000),
                Filter.withLabel("foo", "bar"));
        assertThat(map).hasSize(1);
        assertThat(map.get(key).group()).isEqualTo(key);
        assertThat(map.get(key).samples()).hasSize(6);
        assertThat(map.get(key).labels()).isEmpty();

        map = blockingTs.tsMRange(TimeSeriesRange.fromTimestamps(10, 10000), new MRangeArgs().withLabels(),
                Filter.withLabel("foo", "bar"));
        assertThat(map).hasSize(1);
        assertThat(map.get(key).group()).isEqualTo(key);
        assertThat(map.get(key).samples()).hasSize(6);
        assertThat(map.get(key).labels()).hasSize(3);

        map = blockingTs.tsMGet(new MGetArgs().withLabels(), Filter.withLabelHavingValueFrom("foo", "bar", "baz"));
        assertThat(map).hasSize(2);

        map = blockingTs.tsMGet(Filter.withLabelHavingValueFrom("foo", "bar", "baz"));
        assertThat(map).hasSize(2);

        map = blockingTs.tsMGet(new MGetArgs().selectedLabel("foo"), Filter.withLabelHavingValueFrom("foo", "bar", "baz"));
        assertThat(map).hasSize(2);

        var l = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries(), new RangeArgs().filterByValue(25, 30));
        assertThat(l).hasSize(3);

        map = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(), new MRangeArgs().filterByValue(25, 30),
                Filter.withLabel("foo", "bar"));
        assertThat(map.get(key).samples()).hasSize(3);

        map = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(), new MRangeArgs().filterByValue(25, 30),
                Filter.withLabel("foo"),
                Filter.withLabel("area", 1));
        assertThat(map).hasSize(0);

        l = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries(),
                new RangeArgs().filterByTimestamp(1000, 1001, 2000, 2005));
        assertThat(l).hasSize(3);

        map = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(), new MRangeArgs().filterByTimestamp(1000, 1001, 2000, 2005),
                Filter.withLabel("foo", "bar"));
        assertThat(map.get(key).samples()).hasSize(3);
    }

    @Test
    void testAggregationInRange() {
        blockingTs.tsCreate(key, new CreateArgs()
                .label("area", 1)
                .label("common", "1")
                .label("foo", "bar"));
        blockingTs.tsCreate("sensor", new CreateArgs()
                .label("common", "1")
                .label("foo", "baz"));

        blockingTs.tsAdd(key, 500, 50);
        blockingTs.tsAdd("sensor", 2003, 42);
        blockingTs.tsAdd("sensor", 501, 51);
        blockingTs.tsAdd(key, 1000, 26);
        blockingTs.tsAdd(key, 1001, 27);
        blockingTs.tsAdd(key, 2000, 28);
        blockingTs.tsAdd("sensor", 2001, 29);
        blockingTs.tsAdd(key, 1002, 1002339488832.23477);
        blockingTs.tsAdd("sensor", 1003, 45.56778);
        blockingTs.tsAdd("sensor", 2004, 45);
        blockingTs.tsAdd(key, 3000, 45);

        var l = blockingTs.tsRange(key, TimeSeriesRange.fromTimeSeries(),
                new RangeArgs().aggregation(Aggregation.AVG, Duration.ofMillis(3600000)));
        assertThat(l).hasSize(1);

        var map = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().aggregation(Aggregation.AVG, Duration.ofMillis(3600000)), Filter.withLabel("area", 1));
        assertThat(map.get(key).samples()).hasSize(1);

        map = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().aggregation(Aggregation.MIN, Duration.ofMillis(3600000)).align(1000),
                Filter.withLabel("area", 1));
        assertThat(map.get(key).samples()).hasSize(2);
    }

    @Test
    void testAggregationInRevRange() {
        blockingTs.tsCreate(key, new CreateArgs()
                .label("area", 1)
                .label("common", "1")
                .label("foo", "bar"));
        blockingTs.tsCreate("sensor", new CreateArgs()
                .label("common", "1")
                .label("foo", "baz"));

        blockingTs.tsAdd(key, 500, 50);
        blockingTs.tsAdd("sensor", 2003, 42);
        blockingTs.tsAdd("sensor", 501, 51);
        blockingTs.tsAdd(key, 1000, 26);
        blockingTs.tsAdd(key, 1001, 27);
        blockingTs.tsAdd(key, 2000, 28);
        blockingTs.tsAdd("sensor", 2001, 29);
        blockingTs.tsAdd(key, 1002, 1002339488832.23477);
        blockingTs.tsAdd("sensor", 1003, 45.56778);
        blockingTs.tsAdd("sensor", 2004, 45);
        blockingTs.tsAdd(key, 3000, 45);

        var l = blockingTs.tsRevRange(key, TimeSeriesRange.fromTimeSeries(),
                new RangeArgs().aggregation(Aggregation.AVG, Duration.ofMillis(3600000)));
        assertThat(l).hasSize(1);

        var map = blockingTs.tsMRevRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().aggregation(Aggregation.AVG, Duration.ofMillis(3600000)), Filter.withLabel("area", 1));
        assertThat(map.get(key).samples()).hasSize(1);

        map = blockingTs.tsMRevRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().aggregation(Aggregation.MIN, Duration.ofMillis(3600000)).align(1000),
                Filter.withLabel("area", 1));
        assertThat(map.get(key).samples()).hasSize(2);
    }

    @Test
    void testQueryIndex() {
        blockingTs.tsCreate("telemetry:study:temperature",
                new CreateArgs().label("rooms", "study").label("type", "temperature"));
        blockingTs.tsCreate("telemetry:study:humidity",
                new CreateArgs().label("rooms", "study").label("type", "humidity"));
        blockingTs.tsCreate("telemetry:kitchen:temperature",
                new CreateArgs().label("rooms", "kitchen").label("type", "temperature"));
        blockingTs.tsCreate("telemetry:kitchen:humidity",
                new CreateArgs().label("rooms", "kitchen").label("type", "humidity"));

        var list = blockingTs.tsQueryIndex(Filter.withLabel("rooms", "kitchen"));
        assertThat(list).containsExactlyInAnyOrder("telemetry:kitchen:humidity", "telemetry:kitchen:temperature");

        list = blockingTs.tsQueryIndex(Filter.withLabel("type", "temperature"));
        assertThat(list).containsExactlyInAnyOrder("telemetry:study:temperature", "telemetry:kitchen:temperature");
    }

    @Test
    void testGetAdd() {
        blockingTs.tsCreate("temp:TLV", new CreateArgs().label("type", "temp").label("location", "TLV"));
        blockingTs.tsCreate("temp:JLM", new CreateArgs().label("type", "temp").label("location", "JLM"));

        blockingTs.tsMAdd(
                SeriesSample.from("temp:TLV", 1000, 30),
                SeriesSample.from("temp:TLV", 1010, 35),
                SeriesSample.from("temp:TLV", 1020, 9999),
                SeriesSample.from("temp:TLV", 1030, 40));
        blockingTs.tsMAdd(
                SeriesSample.from("temp:JLM", 1005, 30),
                SeriesSample.from("temp:JLM", 1015, 35),
                SeriesSample.from("temp:JLM", 1025, 9999),
                SeriesSample.from("temp:JLM", 1035, 40));

        assertThat(blockingTs.tsGet("temp:JLM").value).isEqualTo(40);
    }

    @Test
    void testRevRange() {
        blockingTs.tsCreate("temp:TLV", new CreateArgs().label("type", "temp").label("location", "TLV"));

        blockingTs.tsMAdd(
                SeriesSample.from("temp:TLV", 1000, 30),
                SeriesSample.from("temp:TLV", 1010, 40),
                SeriesSample.from("temp:TLV", 1030, 35));

        var list = blockingTs.tsRevRange("temp:TLV", TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(3);
        assertThat(list.getFirst().value).isEqualTo(35);

        var map = blockingTs.tsMRevRange(TimeSeriesRange.fromTimeSeries(), Filter.withLabel("type", "temp"));
        assertThat(map).hasSize(1);
        assertThat(map.get("temp:TLV").samples()).hasSize(3);
    }

    @SuppressWarnings("unchecked")
    @Test
    void groupByAndReducerTest() {
        String a = "stock:a";
        String b = "stock:b";
        blockingTs.tsCreate(a, new CreateArgs().label("type", "stock").label("name", "a"));
        blockingTs.tsCreate(b, new CreateArgs().label("type", "stock").label("name", "b"));

        blockingTs.tsMAdd(SeriesSample.from(a, 1000, 100), SeriesSample.from(a, 1010, 110), SeriesSample.from(a, 1020, 120));
        blockingTs.tsMAdd(SeriesSample.from(b, 1000, 110), SeriesSample.from(b, 1010, 110), SeriesSample.from(a, 1020, 100));
        var res = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().withLabels().groupBy("type", Reducer.MAX),
                Filter.withLabel("type", "stock"));
        assertThat(res).hasSize(1);
        assertThat(res.get("type=stock").labels().get("type")).isEqualTo("stock");
        assertThat(res.get("type=stock").samples().stream().map(s -> s.value)).containsExactlyInAnyOrder(110.0, 110.0, 120.0);
    }

    @SuppressWarnings("unchecked")
    @Test
    void groupByReduceAndAggregationTest() {
        String a = "stock:a";
        String b = "stock:b";
        blockingTs.tsCreate(a, new CreateArgs().label("type", "stock").label("name", "a"));
        blockingTs.tsCreate(b, new CreateArgs().label("type", "stock").label("name", "b"));

        blockingTs.tsMAdd(SeriesSample.from(a, 1000, 100), SeriesSample.from(a, 1010, 110), SeriesSample.from(a, 1020, 120));
        blockingTs.tsMAdd(SeriesSample.from(b, 1000, 120), SeriesSample.from(b, 1010, 110), SeriesSample.from(a, 1020, 100));

        blockingTs.tsMAdd(SeriesSample.from(a, 2000, 200), SeriesSample.from(a, 2010, 210), SeriesSample.from(a, 2020, 220));
        blockingTs.tsMAdd(SeriesSample.from(b, 2000, 220), SeriesSample.from(b, 2010, 210), SeriesSample.from(a, 2020, 200));

        blockingTs.tsMAdd(SeriesSample.from(a, 3000, 300), SeriesSample.from(a, 3010, 310), SeriesSample.from(a, 3020, 320));
        blockingTs.tsMAdd(SeriesSample.from(b, 3000, 320), SeriesSample.from(b, 3010, 310), SeriesSample.from(a, 3020, 300));

        var res = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().withLabels()
                        .aggregation(Aggregation.AVG, Duration.ofMillis(1000))
                        .groupBy("type", Reducer.MIN),
                Filter.withLabel("type", "stock"));
        assertThat(res).hasSize(1);
        assertThat(res.get("type=stock").labels().get("type")).isEqualTo("stock");
        assertThat(res.get("type=stock").samples().stream().map(s -> s.value)).containsExactlyInAnyOrder(110.0, 210.0, 310.0);

        res = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().withLabels()
                        .aggregation(Aggregation.AVG, Duration.ofMillis(1000))
                        .groupBy("type", Reducer.MIN),
                Filter.withLabel("type", "stock"));
        assertThat(res).hasSize(1);
        assertThat(res.get("type=stock").labels().get("type")).isEqualTo("stock");
        assertThat(res.get("type=stock").samples().stream().map(s -> s.value)).containsExactlyInAnyOrder(110.0, 210.0, 310.0);

        res = blockingTs.tsMRange(TimeSeriesRange.fromTimestamps(0, 5000),
                new MRangeArgs().withLabels()
                        .alignUsingRangeStart()
                        .aggregation(Aggregation.AVG, Duration.ofMillis(1000)).count(2)
                        .groupBy("type", Reducer.MIN),
                Filter.withLabel("type", "stock"));
        assertThat(res).hasSize(1);
        assertThat(res.get("type=stock").labels().get("type")).isEqualTo("stock");
        assertThat(res.get("type=stock").samples()).hasSize(2);

        res = blockingTs.tsMRange(TimeSeriesRange.fromTimestamps(0, 5000),
                new MRangeArgs().withLabels()
                        .alignUsingRangeEnd()
                        .aggregation(Aggregation.AVG, Duration.ofMillis(1000)).empty()
                        .groupBy("type", Reducer.MIN),
                Filter.withLabel("type", "stock"));
        assertThat(res).hasSize(1);
    }

    @Test
    void groupByReturningMultipleGroupTest() {
        blockingTs.tsAdd("ts1", 1548149180000L, 90, new AddArgs()
                .label("metric", "cpu").label("metric_name", "system").label("team", "NY"));
        blockingTs.tsAdd("ts1", 1548149185000L, 45);

        blockingTs.tsAdd("ts2", 1548149180000L, 99, new AddArgs()
                .label("metric", "cpu").label("metric_name", "user").label("team", "SF"));

        var res = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().withLabels().groupBy("metric_name", Reducer.MAX),
                Filter.withLabel("metric", "cpu"));
        assertThat(res).hasSize(2);
        assertThat(res.get("metric_name=system").samples()).hasSize(2);
        assertThat(res.get("metric_name=user").samples()).containsExactly(new Sample(1548149180000L, 99));

        // Test query by value
        res = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().withLabels().filterByValue(90, 100),
                Filter.withLabel("metric", "cpu"));
        assertThat(res).hasSize(2);
        assertThat(res.get("ts1").samples()).containsExactly(new Sample(1548149180000L, 90));
        assertThat(res.get("ts2").samples()).containsExactly(new Sample(1548149180000L, 99));

        // Test query using label
        res = blockingTs.tsMRange(TimeSeriesRange.fromTimeSeries(),
                new MRangeArgs().selectedLabels("team"),
                Filter.withLabel("metric", "cpu"));
        assertThat(res).hasSize(2);
        assertThat(res.get("ts1").labels()).containsExactly(entry("team", "NY"));
        assertThat(res.get("ts1").samples()).hasSize(2);
        assertThat(res.get("ts2").labels()).containsExactly(entry("team", "SF"));
        assertThat(res.get("ts2").samples()).containsExactly(new Sample(1548149180000L, 99));
    }

    @Test
    void testSample() {
        Sample s1 = new Sample(1000, 24.5);
        Sample s2 = new Sample(1000, 24.5);
        assertThat(s1.value).isEqualTo(24.5);
        assertThat(s2.timestamp()).isEqualTo(1000);
        assertThat(s1).isEqualTo(s2);
        assertThat(s1.hashCode()).isEqualTo(s2.hashCode());
    }

    @Test
    void testRange() {
        blockingTs.tsCreate("temp", new CreateArgs()
                .chunkSize(1024).compressed().label("location", "TLV").forever());
        blockingTs.tsMAdd(
                SeriesSample.from("temp", 1000, 30),
                SeriesSample.from("temp", 1010, 35),
                SeriesSample.from("temp", 1020, 9999),
                SeriesSample.from("temp", 1030, 40));

        // Filter by value
        assertThat(blockingTs.tsRange("temp", TimeSeriesRange.fromTimeSeries(), new RangeArgs().filterByValue(-100, 100)))
                .hasSize(3);

        // With aggregation
        assertThat(blockingTs.tsRange("temp", TimeSeriesRange.fromTimestamps(0, 2000), new RangeArgs().filterByValue(-100, 100)
                .aggregation(Aggregation.AVG, Duration.ofSeconds(1)).alignUsingRangeEnd()))
                .hasSize(1).containsExactly(new Sample(1000, 35));
    }

    @Test
    void testRangeWithAlignment() {
        String a = "stock:a";
        blockingTs.tsCreate(a, new CreateArgs().label("type", "stock").label("name", "a"));

        blockingTs.tsMAdd(SeriesSample.from(a, 1000, 100), SeriesSample.from(a, 1010, 110), SeriesSample.from(a, 1020, 120));
        blockingTs.tsMAdd(SeriesSample.from(a, 2000, 200), SeriesSample.from(a, 2010, 210), SeriesSample.from(a, 2020, 220));
        blockingTs.tsMAdd(SeriesSample.from(a, 3000, 300), SeriesSample.from(a, 3010, 310), SeriesSample.from(a, 3020, 320));

        var list = blockingTs.tsRange(a, TimeSeriesRange.fromTimeSeries(),
                new RangeArgs().aggregation(Aggregation.MIN, Duration.ofMillis(20)).empty());
        assertThat(list)
                .contains(new Sample(1000, 100), new Sample(1020, 120), new Sample(2000, 200),
                        new Sample(2020, 220), new Sample(3000, 300), new Sample(3020, 320));

        list = blockingTs.tsRange(a, TimeSeriesRange.fromTimeSeries(), new RangeArgs()
                .align(10).count(2)
                .aggregation(Aggregation.MIN, Duration.ofMillis(20)));
        assertThat(list)
                .hasSize(2)
                .contains(new Sample(990, 100), new Sample(1010, 110));

        list = blockingTs.tsRange(a, TimeSeriesRange.fromTimestampToLatest(5), new RangeArgs()
                .alignUsingRangeStart()
                .aggregation(Aggregation.MIN, Duration.ofMillis(20)));
        assertThat(list)
                .contains(new Sample(985, 100), new Sample(1005, 110));

        list = blockingTs.tsRange(a, TimeSeriesRange.fromEarliestToTimestamp(3000), new RangeArgs()
                .alignUsingRangeEnd()
                .aggregation(Aggregation.MIN, Duration.ofMillis(20)));
        assertThat(list)
                .contains(new Sample(1000, 100), new Sample(2000, 200));

    }

    @Test
    void testCreation() {
        blockingTs.tsCreate(key, new CreateArgs().forever().duplicatePolicy(DuplicatePolicy.LAST).uncompressed());

        blockingTs.tsAdd(key, 10, 10);
        blockingTs.tsAdd(key, 10, 20);

        assertThat(blockingTs.tsGet(key).value()).isEqualTo(20L);
    }

    @Test
    void testIncrementWithCreation() {
        blockingTs.tsIncrBy(key, 10, new IncrementArgs().setTimestamp(1000).label("foo", "bar")
                .uncompressed().chunkSize(1024).setRetention(Duration.ofDays(1)));
        assertThat(blockingTs.tsGet(key).value()).isEqualTo(10L);
        assertThat(blockingTs.tsGet(key).timestamp()).isEqualTo(1000);
    }

    @Test
    void testTimeSeriesWithTypeReference() throws InterruptedException {
        var ts = blockingDs.timeseries(new TypeReference<List<Person>>() {
            // Empty on purpose.
        });
        var key = List.of(Person.person1, Person.person2);
        var key2 = List.of(Person.person0);
        ts.tsCreate(key);
        ts.tsCreate(key2, new CreateArgs().setRetention(Duration.of(2678400000L, ChronoUnit.MILLIS)));

        ts.tsAlter(key2, new AlterArgs().chunkSize(1024));

        ts.tsAdd(key, 1626434637914L, 26);
        ts.tsAdd(key, 27);

        Thread.sleep(1); // Do make sure we have a different timestamp
        ts.tsMAdd(SeriesSample.from(key, 51), SeriesSample.from(key, 1626434637915L, 52), SeriesSample.from(key2, 22));

        var list = ts.tsRange(key, TimeSeriesRange.fromTimeSeries());
        assertThat(list).hasSize(4);

        list = ts.tsRange(key2, TimeSeriesRange.fromTimestampToLatest(0));
        assertThat(list).hasSize(1);
    }

}
