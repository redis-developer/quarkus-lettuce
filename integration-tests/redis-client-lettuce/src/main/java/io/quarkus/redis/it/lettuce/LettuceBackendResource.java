package io.quarkus.redis.it.lettuce;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.hash.HashCommands;
import io.quarkus.redis.datasource.hash.ReactiveHashCommands;
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.datasource.keys.KeyScanCursor;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.keys.RedisValueType;
import io.quarkus.redis.datasource.list.ListCommands;
import io.quarkus.redis.datasource.list.ReactiveListCommands;
import io.quarkus.redis.datasource.set.ReactiveSetCommands;
import io.quarkus.redis.datasource.set.SetCommands;
import io.quarkus.redis.datasource.sortedset.ReactiveSortedSetCommands;
import io.quarkus.redis.datasource.sortedset.ScoredValue;
import io.quarkus.redis.datasource.sortedset.SortedSetCommands;
import io.quarkus.redis.datasource.transactions.OptimisticLockingTransactionResult;
import io.quarkus.redis.datasource.transactions.TransactionResult;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.value.ValueCommands;
import io.smallrye.mutiny.Uni;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Response;

@Path("/lettuce")
@ApplicationScoped
public class LettuceBackendResource {

    private final RedisDataSource blocking;
    private final ReactiveRedisDataSource reactive;
    private final ValueCommands<String, String> values;
    private final ReactiveValueCommands<String, String> reactiveValues;
    private final KeyCommands<String> keys;
    private final ReactiveKeyCommands<String> reactiveKeys;
    private final HashCommands<String, String, String> hash;
    private final ReactiveHashCommands<String, String, String> reactiveHash;
    private final ListCommands<String, String> list;
    private final ReactiveListCommands<String, String> reactiveList;
    private final SetCommands<String, String> set;
    private final ReactiveSetCommands<String, String> reactiveSet;
    private final SortedSetCommands<String, String> sortedSet;
    private final ReactiveSortedSetCommands<String, String> reactiveSortedSet;

    @Inject
    public LettuceBackendResource(RedisDataSource ds, ReactiveRedisDataSource reactiveDs) {
        this.blocking = ds;
        this.reactive = reactiveDs;
        this.values = ds.value(String.class);
        this.reactiveValues = reactiveDs.value(String.class);
        this.keys = ds.key(String.class);
        this.reactiveKeys = reactiveDs.key(String.class);
        this.hash = ds.hash(String.class);
        this.reactiveHash = reactiveDs.hash(String.class);
        this.list = ds.list(String.class);
        this.reactiveList = reactiveDs.list(String.class);
        this.set = ds.set(String.class);
        this.reactiveSet = reactiveDs.set(String.class);
        this.sortedSet = ds.sortedSet(String.class);
        this.reactiveSortedSet = reactiveDs.sortedSet(String.class);
    }

    @GET
    @Path("/ping")
    public String ping() {
        Response response = blocking.execute("PING");
        return response.toString();
    }

    @GET
    @Path("/ping/command")
    public String pingCommand() {
        Response response = blocking.execute(Command.PING);
        return response.toString();
    }

    @POST
    @Path("/value/{key}")
    public void setValue(@PathParam("key") String key, String value) {
        values.set(key, value);
    }

    @GET
    @Path("/value/{key}")
    public String getValue(@PathParam("key") String key) {
        return values.get(key);
    }

    @GET
    @Path("/value/lcs/{key1}/{key2}")
    public String lcs(@PathParam("key1") String key1, @PathParam("key2") String key2) {
        return values.lcs(key1, key2) + "," + values.lcsLength(key1, key2);
    }

    @POST
    @Path("/select/{index}")
    public void select(@PathParam("index") long index) {
        blocking.select(index);
    }

    @DELETE
    @Path("/flushall")
    public void flushall() {
        blocking.flushall();
    }

    @GET
    @Path("/reactive/{key}")
    public Uni<String> getReactive(@PathParam("key") String key) {
        return reactiveValues.get(key);
    }

    @GET
    @Path("/key/exists/{key}")
    public boolean keyExists(@PathParam("key") String key) {
        return keys.exists(key);
    }

    @DELETE
    @Path("/key/{key}")
    public int keyDel(@PathParam("key") String key) {
        return keys.del(key);
    }

    @POST
    @Path("/key/expire/{key}/{seconds}")
    public boolean keyExpire(@PathParam("key") String key, @PathParam("seconds") long seconds) {
        return keys.expire(key, seconds);
    }

    @GET
    @Path("/key/ttl/{key}")
    public long keyTtl(@PathParam("key") String key) {
        return keys.ttl(key);
    }

    @POST
    @Path("/key/persist/{key}")
    public boolean keyPersist(@PathParam("key") String key) {
        return keys.persist(key);
    }

    @POST
    @Path("/key/rename/{key}/{newkey}")
    public void keyRename(@PathParam("key") String key, @PathParam("newkey") String newkey) {
        keys.rename(key, newkey);
    }

    @POST
    @Path("/key/copy/{src}/{dst}")
    public boolean keyCopy(@PathParam("src") String src, @PathParam("dst") String dst) {
        return keys.copy(src, dst);
    }

    @GET
    @Path("/key/type/{key}")
    public String keyType(@PathParam("key") String key) {
        return keys.type(key).name();
    }

    @GET
    @Path("/key/scan")
    public Set<String> keyScan(@QueryParam("match") String match) {
        KeyScanArgs args = new KeyScanArgs();
        if (match != null) {
            args.match(match);
        }
        KeyScanCursor<String> cursor = keys.scan(args);
        Set<String> collected = new HashSet<>();
        while (cursor.hasNext()) {
            collected.addAll(cursor.next());
        }
        return collected;
    }

    @GET
    @Path("/key/reactive/ttl/{key}")
    public Uni<Long> keyTtlReactive(@PathParam("key") String key) {
        return reactiveKeys.ttl(key);
    }

    @POST
    @Path("/hash/{key}/{field}")
    public boolean hashSet(@PathParam("key") String key, @PathParam("field") String field, String value) {
        return hash.hset(key, field, value);
    }

    @GET
    @Path("/hash/{key}/{field}")
    public String hashGet(@PathParam("key") String key, @PathParam("field") String field) {
        return hash.hget(key, field);
    }

    @GET
    @Path("/hash/{key}")
    public Map<String, String> hashGetAll(@PathParam("key") String key) {
        return hash.hgetall(key);
    }

    @GET
    @Path("/hash/reactive/{key}/{field}")
    public Uni<String> hashGetReactive(@PathParam("key") String key, @PathParam("field") String field) {
        return reactiveHash.hget(key, field);
    }

    @POST
    @Path("/list/{key}")
    public long listPush(@PathParam("key") String key, String value) {
        return list.lpush(key, value);
    }

    @GET
    @Path("/list/{key}")
    public List<String> listRange(@PathParam("key") String key) {
        return list.lrange(key, 0, -1);
    }

    @GET
    @Path("/list/reactive/{key}")
    public Uni<List<String>> listRangeReactive(@PathParam("key") String key) {
        return reactiveList.lrange(key, 0, -1);
    }

    @POST
    @Path("/set/{key}")
    public int setAdd(@PathParam("key") String key, String value) {
        return set.sadd(key, value);
    }

    @GET
    @Path("/set/{key}")
    public Set<String> setMembers(@PathParam("key") String key) {
        return set.smembers(key);
    }

    @GET
    @Path("/set/ismember/{key}/{member}")
    public boolean setIsMember(@PathParam("key") String key, @PathParam("member") String member) {
        return set.sismember(key, member);
    }

    @GET
    @Path("/set/reactive/{key}")
    public Uni<Long> setCardReactive(@PathParam("key") String key) {
        return reactiveSet.scard(key);
    }

    @POST
    @Path("/sortedset/add/{key}/{score}")
    public boolean sortedSetAdd(@PathParam("key") String key, @PathParam("score") double score, String member) {
        return sortedSet.zadd(key, score, member);
    }

    @GET
    @Path("/sortedset/card/{key}")
    public long sortedSetCard(@PathParam("key") String key) {
        return sortedSet.zcard(key);
    }

    @GET
    @Path("/sortedset/score/{key}/{member}")
    public Double sortedSetScore(@PathParam("key") String key, @PathParam("member") String member) {
        OptionalDouble score = sortedSet.zscore(key, member);
        return score.isPresent() ? score.getAsDouble() : null;
    }

    @GET
    @Path("/sortedset/rank/{key}/{member}")
    public Long sortedSetRank(@PathParam("key") String key, @PathParam("member") String member) {
        OptionalLong rank = sortedSet.zrank(key, member);
        return rank.isPresent() ? rank.getAsLong() : null;
    }

    @POST
    @Path("/sortedset/popmin/{key}")
    public String sortedSetPopMin(@PathParam("key") String key) {
        ScoredValue<String> popped = sortedSet.zpopmin(key);
        return popped.value() + "," + popped.score();
    }

    @GET
    @Path("/sortedset/reactive/score/{key}/{member}")
    public Uni<Double> sortedSetScoreReactive(@PathParam("key") String key, @PathParam("member") String member) {
        return reactiveSortedSet.zscore(key, member);
    }

    @GET
    @Path("/with-connection/client-ids")
    public String withConnectionClientIds() {
        long outside = blocking.execute("CLIENT", "ID").toLong();
        long[] inside = new long[2];
        blocking.withConnection(ds -> {
            inside[0] = ds.execute("CLIENT", "ID").toLong();
            inside[1] = ds.execute("CLIENT", "ID").toLong();
        });
        return inside[0] + "," + inside[1] + "," + outside;
    }

    @GET
    @Path("/with-connection/reactive/client-ids")
    public Uni<String> withConnectionClientIdsReactive() {
        long[] inside = new long[2];
        return reactive.execute("CLIENT", "ID").map(Response::toLong)
                .chain(outside -> reactive.withConnection(ds -> ds.execute("CLIENT", "ID").map(Response::toLong)
                        .invoke(id -> inside[0] = id)
                        .chain(() -> ds.execute("CLIENT", "ID").map(Response::toLong))
                        .invoke(id -> inside[1] = id)
                        .replaceWithVoid())
                        .map(ignored -> inside[0] + "," + inside[1] + "," + outside));
    }

    @GET
    @Path("/with-connection/nested")
    public String withConnectionNested() {
        long[] ids = new long[2];
        blocking.withConnection(outer -> {
            ids[0] = outer.execute("CLIENT", "ID").toLong();
            outer.withConnection(inner -> ids[1] = inner.execute("CLIENT", "ID").toLong());
        });
        return ids[0] + "," + ids[1];
    }

    @POST
    @Path("/with-transaction/blocking/{key}")
    public String withTransactionBlocking(@PathParam("key") String key, String value) {
        TransactionResult result = blocking.withTransaction(tx -> {
            var v = tx.value(String.class, String.class);
            v.set(key, value);
            v.get(key);
        });
        return result.discarded() + "," + result.size() + "," + result.get(1);
    }

    @POST
    @Path("/with-transaction/reactive/{key}")
    public Uni<String> withTransactionReactive(@PathParam("key") String key, String value) {
        return reactive.withTransaction(tx -> {
            var v = tx.value(String.class, String.class);
            return v.set(key, value).chain(() -> v.get(key));
        }).map(result -> result.discarded() + "," + result.size() + "," + result.get(1));
    }

    @POST
    @Path("/with-transaction/discard/{key}")
    public String withTransactionDiscard(@PathParam("key") String key, String value) {
        TransactionResult result = blocking.withTransaction(tx -> {
            tx.value(String.class, String.class).set(key, value);
            tx.discard();
        });
        return result.discarded() + "," + values.get(key);
    }

    @POST
    @Path("/with-transaction/optimistic/{key}")
    public String withTransactionOptimistic(@PathParam("key") String key, String suffix) {
        OptimisticLockingTransactionResult<String> result = blocking.withTransaction(
                preTx -> preTx.value(String.class, String.class).get(key),
                (current, tx) -> tx.value(String.class, String.class).set(key, current + suffix),
                key);
        return result.discarded() + "," + result.getPreTransactionResult() + "," + values.get(key);
    }

    @POST
    @Path("/with-transaction/key/{key}")
    public String withTransactionKey(@PathParam("key") String key) {
        values.set(key, "v");
        TransactionResult result = blocking.withTransaction(tx -> {
            var k = tx.key(String.class);
            k.exists(key);
            k.expire(key, 100);
            k.ttl(key);
            k.type(key);
        });
        boolean exists = result.get(0);
        boolean expired = result.get(1);
        long ttl = result.get(2);
        RedisValueType type = result.get(3);
        return result.discarded() + "," + result.size() + "," + exists + "," + expired + "," + (ttl > 0) + "," + type;
    }

    @POST
    @Path("/with-transaction/sortedset/{key}")
    public String withTransactionSortedSet(@PathParam("key") String key) {
        TransactionResult result = blocking.withTransaction(tx -> {
            var s = tx.sortedSet(String.class);
            s.zadd(key, 1.0, "a");
            s.zadd(key, Map.of("b", 2.0, "c", 3.0));
            s.zcard(key);
            s.zpopmin(key);
        });
        boolean added = result.get(0);
        int addedCount = result.get(1);
        long card = result.get(2);
        ScoredValue<String> min = result.get(3);
        return result.discarded() + "," + result.size() + "," + added + "," + addedCount + "," + card
                + "," + min.value() + "," + min.score();
    }
}
