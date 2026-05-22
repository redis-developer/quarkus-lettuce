package io.quarkus.redis.it.lettuce;

import java.util.HashSet;
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
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.keys.KeyScanArgs;
import io.quarkus.redis.datasource.keys.KeyScanCursor;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.quarkus.redis.datasource.value.ValueCommands;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Response;

@Path("/lettuce")
@ApplicationScoped
public class LettuceBackendResource {

    private final RedisDataSource blocking;
    private final ReactiveRedisDataSource reactive;
    private final ValueCommands<String, String> values;
    private final ReactiveValueCommands<String, String> reactiveValues;
    private final KeyCommands<String> keys;
    private final ReactiveKeyCommands<String> reactiveKeys;

    @Inject
    public LettuceBackendResource(RedisDataSource ds, ReactiveRedisDataSource reactiveDs) {
        this.blocking = ds;
        this.reactive = reactiveDs;
        this.values = ds.value(String.class);
        this.reactiveValues = reactiveDs.value(String.class);
        this.keys = ds.key(String.class);
        this.reactiveKeys = reactiveDs.key(String.class);
    }

    @GET
    @Path("/ping")
    public String ping() {
        Response response = blocking.execute("PING");
        return response.toString();
    }

    @GET
    @Path("/ping/mutiny")
    public String pingMutiny() {
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
}
