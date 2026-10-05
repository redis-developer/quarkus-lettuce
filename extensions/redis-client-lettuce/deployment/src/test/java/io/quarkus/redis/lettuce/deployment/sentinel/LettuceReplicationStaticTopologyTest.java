package io.quarkus.redis.lettuce.deployment.sentinel;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.value.ValueCommands;
import io.quarkus.redis.lettuce.deployment.RedisSentinelTestResource;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.QuarkusTestResource;

/**
 * {@code topology=static} takes the configured hosts as the nodes. Unlike the Vert.x client, which trusts the order
 * (first the master), Lettuce asks each node for its role: the hosts are configured in the wrong order here and the
 * writes still reach the master.
 */
@QuarkusTestResource(RedisSentinelTestResource.class)
public class LettuceReplicationStaticTopologyTest {

    @RegisterExtension
    static final QuarkusExtensionTest test = new QuarkusExtensionTest()
            // the test runs inside the application: its helper must be part of the archive
            .withApplicationRoot(jar -> jar.addClass(MasterReplicaNodes.class))
            .overrideConfigKey("quarkus.redis.hosts", "${redis.replication.reversed-hosts}")
            .overrideConfigKey("quarkus.redis.client-type", "replication")
            .overrideConfigKey("quarkus.redis.topology", "static");

    @Inject
    RedisDataSource blocking;

    @ConfigProperty(name = "redis.replication.hosts")
    String replicationHosts;

    @ConfigProperty(name = "redis.sentinel.hosts")
    String sentinelHosts;

    @Test
    void asksTheNodesForTheirRoles() {
        try (MasterReplicaNodes nodes = new MasterReplicaNodes(replicationHosts, sentinelHosts,
                RedisSentinelTestResource.MASTER_NAME)) {
            ValueCommands<String, String> values = blocking.value(String.class);
            values.set("replication:static", "v");
            assertThat(values.get("replication:static")).isEqualTo("v");
            assertThat(blocking.execute("CONFIG", "GET", "port").get(1).toInteger()).isEqualTo(nodes.masterPort());
        }
    }
}
