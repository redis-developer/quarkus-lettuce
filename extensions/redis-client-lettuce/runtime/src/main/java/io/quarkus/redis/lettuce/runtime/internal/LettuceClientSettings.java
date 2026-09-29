package io.quarkus.redis.lettuce.runtime.internal;

import static io.quarkus.vertx.core.runtime.SSLConfigHelper.configureJksKeyCertOptions;
import static io.quarkus.vertx.core.runtime.SSLConfigHelper.configureJksTrustOptions;
import static io.quarkus.vertx.core.runtime.SSLConfigHelper.configurePemKeyCertOptions;
import static io.quarkus.vertx.core.runtime.SSLConfigHelper.configurePemTrustOptions;
import static io.quarkus.vertx.core.runtime.SSLConfigHelper.configurePfxKeyCertOptions;
import static io.quarkus.vertx.core.runtime.SSLConfigHelper.configurePfxTrustOptions;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.jboss.logging.Logger;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisCredentialsProvider;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.StaticCredentialsProvider;
import io.quarkus.redis.runtime.client.config.RedisClientConfig;
import io.quarkus.tls.TlsConfiguration;
import io.quarkus.tls.TlsConfigurationRegistry;
import io.quarkus.tls.runtime.config.TlsConfigUtils;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.TrustOptions;

/**
 * The Lettuce {@link RedisURI} and {@link ClientOptions} derived from a {@code quarkus.redis[.<name>].*} client
 * configuration: the (first) host, the credentials and the TLS settings.
 * <p>
 * The credentials and TLS properties are interpreted exactly as the Vert.x Redis client interprets them:
 * <ul>
 * <li>the credentials encoded in the URI take precedence over the {@code password} property, and are parsed the way
 * the Vert.x client parses them (see {@code UserInfo});</li>
 * <li>instead of re-implementing the {@code quarkus.redis.tls.*} and TLS registry handling, the settings are first
 * applied to a throwaway Vert.x {@link NetClientOptions} with the helpers the Vert.x backend uses, and the outcome
 * (trust and key material, trust-all, hostname verification, protocols and cipher suites) is then mapped onto the
 * Lettuce {@link SslOptions} and {@link SslVerifyMode}.</li>
 * </ul>
 */
public final class LettuceClientSettings {

    private static final Logger LOGGER = Logger.getLogger(LettuceClientSettings.class);

    /**
     * Accepts every certificate chain, like the Vert.x trust-all option. It is deliberately a plain (not extended)
     * {@link X509TrustManager}: the JDK wraps it and keeps performing the endpoint identification when a hostname
     * verification algorithm is set, so {@code trust-all} disables the chain validation only, as with the Vert.x
     * client.
     */
    private static final X509TrustManager TRUST_ALL = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    private final RedisURI redisUri;
    private final ClientOptions clientOptions;

    private LettuceClientSettings(RedisURI redisUri, ClientOptions clientOptions) {
        this.redisUri = redisUri;
        this.clientOptions = clientOptions;
    }

    /**
     * Builds the Lettuce settings for a Redis client.
     *
     * @param name the Quarkus Redis client name
     * @param config the client configuration
     * @param host the Redis URI to connect to (the first configured host)
     * @param vertx the Vert.x instance, used to load the configured trust and key material
     * @param tlsRegistry the TLS registry providing the named TLS configurations
     * @return the settings, never {@code null}
     * @throws IllegalStateException if the referenced named TLS configuration does not exist or its material cannot
     *         be loaded
     */
    public static LettuceClientSettings create(String name, RedisClientConfig config, URI host, Vertx vertx,
            TlsConfigurationRegistry tlsRegistry) {
        RedisURI redisUri = RedisURI.create(host);
        // Lettuce parsed the credentials of the URI its own way; replace them with the Vert.x interpretation.
        redisUri.setCredentialsProvider(credentials(UserInfo.parse(host), config.password().orElse(null)));

        NetClientOptions net = new NetClientOptions();
        configureTls(name, config, tlsRegistry, net, host);

        ClientOptions.Builder options = ClientOptions.builder();
        redisUri.setSsl(net.isSsl());
        if (net.isSsl()) {
            redisUri.setVerifyPeer(verifyMode(net));
            options.sslOptions(sslOptions(name, net, vertx));
        }
        return new LettuceClientSettings(redisUri, options.build());
    }

    public RedisURI redisUri() {
        return redisUri;
    }

    public ClientOptions clientOptions() {
        return clientOptions;
    }

    /**
     * Combines the credentials of the URI with the {@code password} property. Like with the Vert.x Redis client, the
     * property is the default password: a password encoded in the URI takes precedence. The user name, if any,
     * always comes from the URI; a user name without password does not authenticate.
     *
     * @param userInfo the credentials encoded in the URI
     * @param passwordProperty the {@code password} property, {@code null} when not set
     */
    static RedisCredentialsProvider credentials(UserInfo userInfo, String passwordProperty) {
        String password = userInfo.password() != null ? userInfo.password() : passwordProperty;
        return new StaticCredentialsProvider(userInfo.username(), password == null ? null : password.toCharArray());
    }

    /**
     * The credentials encoded in a Redis URI, interpreted as the Vert.x Redis client does: {@code user:password@},
     * {@code :password@} or {@code user@} (a lone user info is the user name, not the password as in Lettuce), each
     * part URL-decoded, with the {@code user} and {@code password} query parameters as fallbacks. Empty parts count
     * as absent.
     * <p>
     * The query parameters deliberately differ from the Vert.x client in one respect: they are split on the raw
     * query and then URL-decoded, the same way as the user info. The Vert.x client splits the already decoded query
     * and does not decode the values, so a percent-encoded {@code &} or {@code =} in a value truncates the value
     * there, and a {@code +} stays a plus sign there while it is a space here.
     *
     * @param username the user name, {@code null} when the URI does not carry one
     * @param password the password, {@code null} when the URI does not carry one
     */
    record UserInfo(String username, String password) {

        static final UserInfo NONE = new UserInfo(null, null);

        static UserInfo parse(URI uri) {
            String username = null;
            String password = null;
            String userInfo = uri.getRawUserInfo();
            if (userInfo != null && !userInfo.isEmpty()) {
                int colon = userInfo.indexOf(':');
                if (colon < 0) {
                    username = decode(userInfo);
                } else {
                    if (colon > 0) {
                        username = decode(userInfo.substring(0, colon));
                    }
                    password = decode(userInfo.substring(colon + 1));
                }
            }
            Map<String, String> query = queryParameters(uri);
            if (username == null) {
                username = query.get("user");
            }
            if (password == null) {
                password = query.get("password");
            }
            return new UserInfo(emptyToNull(username), emptyToNull(password));
        }

        private static Map<String, String> queryParameters(URI uri) {
            String query = uri.getRawQuery();
            if (query == null) {
                return Map.of();
            }
            Map<String, String> parameters = new HashMap<>();
            for (String parameter : query.split("&")) {
                int equals = parameter.indexOf('=');
                if (equals > 0) {
                    parameters.put(decode(parameter.substring(0, equals)), decode(parameter.substring(equals + 1)));
                }
            }
            return parameters;
        }

        private static String decode(String value) {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        }

        private static String emptyToNull(String value) {
            return value == null || value.isEmpty() ? null : value;
        }
    }

    /**
     * Mirrors {@code VertxRedisClientFactory.configureTLS} (and the TLS-relevant part of its TCP configuration) for
     * a single host, so that both backends read the {@code tls.*}, {@code tls-configuration-name} and
     * {@code tcp.secure-transport-protocols} properties the same way.
     */
    private static void configureTls(String name, RedisClientConfig config, TlsConfigurationRegistry tlsRegistry,
            NetClientOptions net, URI host) {
        TlsConfiguration configuration = null;
        boolean defaultTrustAll = false;

        boolean tlsFromHosts = "rediss".equals(host.getScheme());

        // Check if we have a named TLS configuration or a default configuration:
        if (config.tlsConfigurationName().isPresent()) {
            Optional<TlsConfiguration> maybeConfiguration = tlsRegistry.get(config.tlsConfigurationName().get());
            if (maybeConfiguration.isEmpty()) {
                throw new IllegalStateException("Unable to find the TLS configuration "
                        + config.tlsConfigurationName().get() + " for the Redis client " + name + ".");
            }
            configuration = maybeConfiguration.get();
        } else if (tlsRegistry.getDefault().isPresent() && (tlsRegistry.getDefault().get().isTrustAll())) {
            defaultTrustAll = tlsRegistry.getDefault().get().isTrustAll();
            if (defaultTrustAll) {
                LOGGER.warn("The default TLS configuration is set to trust all certificates. This is a security risk."
                        + "Please use a named TLS configuration for the Redis client " + name + " to avoid this warning.");
            }
        }

        if (configuration != null && !tlsFromHosts) {
            LOGGER.warnf("The Redis client %s is configured with a named TLS configuration but the hosts are not " +
                    "using the `rediss://` scheme - Disabling TLS", name);
        }

        config.tcp().secureTransportProtocols().ifPresent(net::setEnabledSecureTransportProtocols);

        // Apply the configuration
        if (configuration != null) {
            TlsConfigUtils.configure(net, configuration);
            net.setSsl(tlsFromHosts);
        } else {
            String verificationAlgorithm = config.tls().hostnameVerificationAlgorithm();
            if ("NONE".equalsIgnoreCase(verificationAlgorithm)) {
                net.setHostnameVerificationAlgorithm("");
            } else {
                net.setHostnameVerificationAlgorithm(verificationAlgorithm);
            }
            net.setSsl(config.tls().enabled() || tlsFromHosts);
            net.setTrustAll(config.tls().trustAll() || defaultTrustAll);

            configurePemTrustOptions(net, config.tls().trustCertificatePem());
            configureJksTrustOptions(net, config.tls().trustCertificateJks());
            configurePfxTrustOptions(net, config.tls().trustCertificatePfx());

            configurePemKeyCertOptions(net, config.tls().keyCertificatePem());
            configureJksKeyCertOptions(net, config.tls().keyCertificateJks());
            configurePfxKeyCertOptions(net, config.tls().keyCertificatePfx());
        }
    }

    /**
     * Maps the Vert.x hostname verification algorithm onto the Lettuce {@link SslVerifyMode}: an empty algorithm, or
     * {@code NONE} as the TLS registry passes it on verbatim from {@code hostname-verification-algorithm}, verifies
     * the certificate chain only; any other algorithm additionally verifies the host name (Lettuce only supports the
     * HTTPS algorithm). Trust-all is not a verify mode but a trust manager (see {@link #TRUST_ALL}), so that, like
     * with the Vert.x client, it does not switch the hostname verification off.
     */
    static SslVerifyMode verifyMode(NetClientOptions net) {
        String algorithm = net.getHostnameVerificationAlgorithm();
        if (algorithm == null || algorithm.isEmpty() || "NONE".equalsIgnoreCase(algorithm)) {
            return SslVerifyMode.CA;
        }
        return SslVerifyMode.FULL;
    }

    private static SslOptions sslOptions(String name, NetClientOptions net, Vertx vertx) {
        SslOptions.Builder ssl = SslOptions.builder();
        try {
            if (net.isTrustAll()) {
                ssl.trustManager(TrustOptions.wrap(TRUST_ALL).getTrustManagerFactory(vertx));
            } else if (net.getTrustOptions() != null) {
                TrustManagerFactory trustManagerFactory = net.getTrustOptions().getTrustManagerFactory(vertx);
                if (trustManagerFactory != null) {
                    ssl.trustManager(trustManagerFactory);
                }
            }
            if (net.getKeyCertOptions() != null) {
                KeyManagerFactory keyManagerFactory = net.getKeyCertOptions().getKeyManagerFactory(vertx);
                if (keyManagerFactory != null) {
                    ssl.keyManager(keyManagerFactory);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to load the TLS trust or key material of the Redis client " + name, e);
        }
        if (net.getEnabledSecureTransportProtocols() != null && !net.getEnabledSecureTransportProtocols().isEmpty()) {
            ssl.protocols(net.getEnabledSecureTransportProtocols().toArray(String[]::new));
        }
        if (net.getEnabledCipherSuites() != null && !net.getEnabledCipherSuites().isEmpty()) {
            ssl.cipherSuites(net.getEnabledCipherSuites().toArray(String[]::new));
        }
        ssl.handshakeTimeout(Duration.of(net.getSslHandshakeTimeout(), net.getSslHandshakeTimeoutUnit().toChronoUnit()));
        return ssl.build();
    }
}
