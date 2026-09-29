package io.quarkus.redis.lettuce.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import jakarta.inject.Inject;

import org.jboss.logmanager.ExtLogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.lettuce.core.api.StatefulRedisConnection;
import io.quarkus.redis.lettuce.runtime.internal.LettuceRecorder;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.QuarkusTestResource;

/**
 * Verifies that the Lettuce backend warns at startup about the configured properties it does not apply, and does
 * not report the ones it applies: the password and the TLS settings (including the client key and certificate) are
 * set and honoured, since the server requires a password and mutual TLS, and the pool size is applied to the
 * connection pool, so none of them must appear in the warning.
 */
@QuarkusTestResource(RedisTlsTestResource.class)
public class LettuceUnsupportedConfigurationWarningTest {

    private static final String CERTS = RedisTlsTestResource.CERTS_DIR + "/" + RedisTlsTestResource.CERT_NAME;

    @RegisterExtension
    static final QuarkusExtensionTest unitTest = new QuarkusExtensionTest()
            .withEmptyApplication()
            // two URIs to the same server, only the first one is used
            .overrideConfigKey("quarkus.redis.hosts",
                    "rediss://${redis.mtls.host}:${redis.mtls.port}/0,rediss://${redis.mtls.host}:${redis.mtls.port}/1")
            .overrideConfigKey("quarkus.redis.password", "${redis.mtls.password}")
            .overrideConfigKey("quarkus.redis.tls.trust-certificate-pem.certs", CERTS + "-client-ca.crt")
            .overrideConfigKey("quarkus.redis.tls.key-certificate-pem", "true")
            .overrideConfigKey("quarkus.redis.tls.key-certificate-pem.certs", CERTS + "-client.crt")
            .overrideConfigKey("quarkus.redis.tls.key-certificate-pem.keys", CERTS + "-client.key")
            .overrideConfigKey("quarkus.redis.client-type", "cluster")
            .overrideConfigKey("quarkus.redis.max-pool-size", "10")
            .overrideConfigKey("quarkus.redis.max-waiting-handlers", "4096")
            .overrideConfigKey("quarkus.redis.reconnect-attempts", "3")
            .overrideConfigKey("quarkus.redis.client-name", "my-app")
            .overrideConfigKey("quarkus.redis.tcp.connection-timeout", "5s")
            .setLogRecordPredicate(record -> LettuceRecorder.class.getName().equals(record.getLoggerName())
                    && record.getLevel().intValue() >= Level.WARNING.intValue())
            .assertLogRecords(records -> assertThat(records)
                    .extracting(LettuceUnsupportedConfigurationWarningTest::message)
                    .singleElement().asString()
                    .contains("Lettuce Redis client '<default>'")
                    .contains("quarkus.redis.hosts (only the first URI is used)")
                    .contains("quarkus.redis.client-type (only standalone is supported)")
                    .contains("quarkus.redis.max-waiting-handlers")
                    // honoured by the connection pool, so not reported
                    .doesNotContain("max-pool-size")
                    .contains("quarkus.redis.reconnect-attempts")
                    .contains("quarkus.redis.client-name")
                    .contains("quarkus.redis.tcp.connection-timeout")
                    .doesNotContain("password")
                    .doesNotContain("tls"));

    @Inject
    StatefulRedisConnection<String, String> connection;

    @Test
    void passwordAndTlsAreApplied() {
        assertThat(connection.sync().ping()).isEqualTo("PONG");
    }

    private static String message(LogRecord record) {
        return record instanceof ExtLogRecord ext ? ext.getFormattedMessage() : record.getMessage();
    }
}
