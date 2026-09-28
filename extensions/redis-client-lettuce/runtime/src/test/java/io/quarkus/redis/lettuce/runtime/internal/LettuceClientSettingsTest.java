package io.quarkus.redis.lettuce.runtime.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisCredentials;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.StaticCredentialsProvider;
import io.quarkus.redis.lettuce.runtime.internal.LettuceClientSettings.UserInfo;
import io.vertx.core.net.NetClientOptions;

/**
 * Unit tests for the URI credential parsing, the password precedence and the peer verification mapping of
 * {@link LettuceClientSettings}, all of which must match the Vert.x Redis client.
 */
class LettuceClientSettingsTest {

    @Test
    void parsesUserInfoLikeVertx() {
        assertThat(UserInfo.parse(URI.create("redis://localhost:6379")))
                .isEqualTo(UserInfo.NONE);
        assertThat(UserInfo.parse(URI.create("redis://user:pass@localhost:6379")))
                .isEqualTo(new UserInfo("user", "pass"));
        assertThat(UserInfo.parse(URI.create("redis://:pass@localhost:6379")))
                .isEqualTo(new UserInfo(null, "pass"));
        // a lone user info is the user name (Vert.x), not the password (Lettuce)
        assertThat(UserInfo.parse(URI.create("redis://user@localhost:6379")))
                .isEqualTo(new UserInfo("user", null));
        // an empty password counts as absent, so the password property applies
        assertThat(UserInfo.parse(URI.create("redis://user:@localhost:6379")))
                .isEqualTo(new UserInfo("user", null));
        // parts are URL-decoded after splitting, so an encoded colon or at-sign is not a separator
        assertThat(UserInfo.parse(URI.create("redis://us%40er:p%3Ass@localhost:6379")))
                .isEqualTo(new UserInfo("us@er", "p:ss"));
        // the Vert.x query parameters are fallbacks
        assertThat(UserInfo.parse(URI.create("redis://localhost:6379?user=u&password=p%26q")))
                .isEqualTo(new UserInfo("u", "p&q"));
        assertThat(UserInfo.parse(URI.create("redis://a:b@localhost:6379?user=u&password=p")))
                .isEqualTo(new UserInfo("a", "b"));
    }

    @Test
    void uriPasswordWinsOverProperty() {
        RedisCredentials fromUri = resolve(new UserInfo("user", "uri-pass"), Optional.of("property-pass"));
        assertThat(fromUri.getUsername()).isEqualTo("user");
        assertThat(fromUri.getPassword()).containsExactly("uri-pass".toCharArray());

        RedisCredentials fromProperty = resolve(new UserInfo("user", null), Optional.of("property-pass"));
        assertThat(fromProperty.getUsername()).isEqualTo("user");
        assertThat(fromProperty.getPassword()).containsExactly("property-pass".toCharArray());

        RedisCredentials defaultUser = resolve(UserInfo.NONE, Optional.of("property-pass"));
        assertThat(defaultUser.hasUsername()).isFalse();
        assertThat(defaultUser.getPassword()).containsExactly("property-pass".toCharArray());

        RedisCredentials none = resolve(new UserInfo("user", null), Optional.empty());
        assertThat(none.getUsername()).isEqualTo("user");
        assertThat(none.hasPassword()).isFalse();
    }

    private static RedisCredentials resolve(UserInfo userInfo, Optional<String> password) {
        return ((StaticCredentialsProvider) LettuceClientSettings.credentials(userInfo, password)).resolveCredentialsNow();
    }

    @Test
    void mapsPeerVerificationIndependentlyOfTrustAll() {
        NetClientOptions net = new NetClientOptions();
        // Vert.x default: no hostname verification, certificate chain verified
        assertThat(LettuceClientSettings.verifyMode(net)).isEqualTo(SslVerifyMode.CA);

        net.setHostnameVerificationAlgorithm("HTTPS");
        assertThat(LettuceClientSettings.verifyMode(net)).isEqualTo(SslVerifyMode.FULL);

        // trust-all only affects the chain validation (through the trust manager), never the hostname verification
        net.setTrustAll(true);
        assertThat(LettuceClientSettings.verifyMode(net)).isEqualTo(SslVerifyMode.FULL);

        net.setHostnameVerificationAlgorithm("");
        assertThat(LettuceClientSettings.verifyMode(net)).isEqualTo(SslVerifyMode.CA);
    }
}
