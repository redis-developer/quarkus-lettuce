package io.quarkus.redis.demo.lettuce;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.value.ValueCommands;

@Path("/api")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class DemoResource {

    private final RedisDataSource ds;
    private final ValueCommands<String, String> values;
    private final KeyCommands<String> keys;
    private final String backend;

    @Inject
    public DemoResource(RedisDataSource ds,
            @ConfigProperty(name = "demo.backend", defaultValue = "vertx") String backend) {
        this.ds = ds;
        this.values = ds.value(String.class);
        this.keys = ds.key(String.class);
        this.backend = backend;
    }

    @GET
    @Path("/backend")
    public BackendInfo backend() {
        return new BackendInfo(backend);
    }

    @GET
    @Path("/value/{key}")
    public ValueResponse getValue(@PathParam("key") String key) {
        return new ValueResponse(values.get(key));
    }

    @POST
    @Path("/value/{key}")
    public ValueResponse setValue(@PathParam("key") String key, String value) {
        values.set(key, value);
        return new ValueResponse(value);
    }

    @POST
    @Path("/counter/{key}")
    public CounterResponse incrCounter(@PathParam("key") String key) {
        long newValue = ds.execute("INCR", key).toLong();
        return new CounterResponse(newValue);
    }

    @GET
    @Path("/counter/{key}")
    public CounterResponse getCounter(@PathParam("key") String key) {
        String raw = values.get(key);
        return new CounterResponse(raw == null ? 0 : Long.parseLong(raw));
    }

    @POST
    @Path("/key/ttl/{key}/{seconds}")
    public TtlResponse setTtl(@PathParam("key") String key, @PathParam("seconds") long seconds) {
        keys.expire(key, seconds);
        return new TtlResponse(keys.ttl(key));
    }

    @GET
    @Path("/key/ttl/{key}")
    public TtlResponse getTtl(@PathParam("key") String key) {
        return new TtlResponse(keys.ttl(key));
    }

    @DELETE
    @Path("/key/{key}")
    public DeletedResponse del(@PathParam("key") String key) {
        return new DeletedResponse(keys.del(key));
    }

    @DELETE
    @Path("/flushall")
    public void flushall() {
        ds.flushall();
    }

    @GET
    @Path("/with-connection/client-ids")
    public PinningResponse withConnectionClientIds() {
        long outside = ds.execute("CLIENT", "ID").toLong();
        long[] inside = new long[2];
        ds.withConnection(pinned -> {
            inside[0] = pinned.execute("CLIENT", "ID").toLong();
            inside[1] = pinned.execute("CLIENT", "ID").toLong();
        });
        return new PinningResponse(inside[0], inside[1], outside);
    }

    @GET
    @Path("/with-connection/nested")
    public NestedResponse withConnectionNested() {
        long[] ids = new long[2];
        ds.withConnection(outer -> {
            ids[0] = outer.execute("CLIENT", "ID").toLong();
            outer.withConnection(inner -> ids[1] = inner.execute("CLIENT", "ID").toLong());
        });
        return new NestedResponse(ids[0], ids[1]);
    }

    public record BackendInfo(String backend) {
    }

    public record ValueResponse(String value) {
    }

    public record CounterResponse(long value) {
    }

    public record TtlResponse(long ttl) {
    }

    public record DeletedResponse(int deleted) {
    }

    public record PinningResponse(long inside1, long inside2, long outside) {
    }

    public record NestedResponse(long outer, long inner) {
    }
}
