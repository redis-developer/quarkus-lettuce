package io.quarkus.redis.deployment.client;

import static io.quarkus.redis.deployment.client.RedisClientProcessor.REDIS_CLIENT_ANNOTATION;
import static io.quarkus.redis.runtime.client.config.RedisConfig.DEFAULT_CLIENT_NAME;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;

import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassType;
import org.jboss.jandex.DotName;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;

import io.quarkus.arc.ActiveResult;
import io.quarkus.arc.deployment.BeanDiscoveryFinishedBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.processor.InjectionPointInfo;
import io.quarkus.bootstrap.classloading.QuarkusClassLoader;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Produce;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ShutdownContextBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ExcludeConfigBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import io.quarkus.deployment.pkg.builditem.ArtifactResultBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;
import io.quarkus.redis.runtime.client.lettuce.LettuceRecorder;
import io.quarkus.runtime.configuration.ConfigurationException;
import io.quarkus.vertx.deployment.VertxBuildItem;

/**
 * Deployment processor that registers Lettuce-based Redis CDI beans as synthetic beans.
 * <p>
 * Produces per named client:
 * <ul>
 * <li>{@code io.lettuce.core.RedisClient}</li>
 * <li>{@code io.lettuce.core.api.StatefulRedisConnection}</li>
 * </ul>
 * And a shared {@code io.lettuce.core.resource.ClientResources} backed by Vert.x event loops.
 * <p>
 * Lettuce classes are referenced by {@link DotName} because Lettuce is an optional
 * runtime dependency, not available on the deployment module classpath.
 */
public class LettuceProcessor {

    private static final DotName LETTUCE_REDIS_CLIENT = DotName.createSimple("io.lettuce.core.RedisClient");
    private static final DotName LETTUCE_STATEFUL_CONNECTION = DotName
            .createSimple("io.lettuce.core.api.StatefulRedisConnection");
    private static final DotName LETTUCE_CLIENT_RESOURCES = DotName.createSimple("io.lettuce.core.resource.ClientResources");

    private static final Type STATEFUL_CONNECTION_STRING_STRING = ParameterizedType.create(
            LETTUCE_STATEFUL_CONNECTION,
            new Type[] {
                    ClassType.create(DotName.createSimple("java.lang.String")),
                    ClassType.create(DotName.createSimple("java.lang.String"))
            },
            null);

    private static final List<DotName> LETTUCE_INJECTION_TYPES = List.of(
            LETTUCE_REDIS_CLIENT,
            LETTUCE_STATEFUL_CONNECTION,
            LETTUCE_CLIENT_RESOURCES);

    /**
     * The Lettuce backend pulls in {@code io.lettuce.core.metrics.DefaultCommandLatencyCollector},
     * whose method signatures reference {@code org.LatencyUtils.PauseDetector} and
     * {@code org.HdrHistogram.Histogram}. Both are declared {@code <optional>true</optional>} by
     * Lettuce so they are absent from the application classpath unless the user adds them. In JVM
     * mode Lettuce silently disables latency tracking, but native-image link-at-build-time analysis
     * rejects the unresolved references. Fail early with an actionable message instead of letting
     * the native-image build error out on a cryptic {@code NoClassDefFoundError}.
     */
    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Produce(ArtifactResultBuildItem.class)
    public void validateLettuceNativeDependencies(RedisBackendBuildItem backend) {
        validateNativeDependencies(backend.isLettuce(),
                QuarkusClassLoader.isClassPresentAtRuntime("org.LatencyUtils.PauseDetector"),
                QuarkusClassLoader.isClassPresentAtRuntime("org.HdrHistogram.Histogram"));
    }

    /**
     * Throws {@link ConfigurationException} if the Lettuce backend is selected and one or both of
     * the optional Lettuce metrics dependencies are missing. No-op for the Vert.x backend or when
     * both dependencies are present. Package-private for testing.
     */
    static void validateNativeDependencies(boolean lettuceBackend, boolean latencyUtilsPresent,
            boolean hdrHistogramPresent) {
        if (!lettuceBackend) {
            return;
        }
        String error = buildNativeDependenciesError(latencyUtilsPresent, hdrHistogramPresent);
        if (error != null) {
            throw new ConfigurationException(error);
        }
    }

    /**
     * Returns the actionable error message when one or both of the optional Lettuce metrics
     * dependencies are missing from the application classpath, or {@code null} if both are
     * present. Package-private for testing.
     */
    static String buildNativeDependenciesError(boolean latencyUtilsPresent, boolean hdrHistogramPresent) {
        if (latencyUtilsPresent && hdrHistogramPresent) {
            return null;
        }
        StringBuilder missing = new StringBuilder();
        if (!latencyUtilsPresent) {
            missing.append("\n            <dependency>\n")
                    .append("                <groupId>org.latencyutils</groupId>\n")
                    .append("                <artifactId>LatencyUtils</artifactId>\n")
                    .append("            </dependency>");
        }
        if (!hdrHistogramPresent) {
            missing.append("\n            <dependency>\n")
                    .append("                <groupId>org.hdrhistogram</groupId>\n")
                    .append("                <artifactId>HdrHistogram</artifactId>\n")
                    .append("            </dependency>");
        }
        return "The Lettuce Redis backend requires the optional 'org.latencyutils:LatencyUtils' and "
                + "'org.hdrhistogram:HdrHistogram' dependencies on the runtime classpath when building "
                + "a native image. Add the following to your application pom.xml:"
                + missing;
    }

    @BuildStep
    public void registerNativeImageHints(RedisBackendBuildItem backend,
            BuildProducer<ExcludeConfigBuildItem> excludeConfig,
            BuildProducer<RuntimeInitializedClassBuildItem> runtimeInit) {
        if (!backend.isLettuce()) {
            return;
        }
        // Lettuce ships a native-image.properties that pins DefaultCommandLatencyCollector and its
        // NoOpPauseDetectorWrapper for build-time initialization. That transitively triggers the
        // DefaultPauseDetectorWrapper <clinit>, which references the optional LatencyUtils dependency
        // (org.LatencyUtils.PauseDetector). LatencyUtils is not on our classpath, so the build fails.
        // Drop the vendor file and pin the affected classes to runtime initialization instead.
        excludeConfig.produce(new ExcludeConfigBuildItem("io\\.lettuce\\.lettuce-core",
                "/META-INF/native-image/io\\.lettuce/lettuce-core/native-image\\.properties"));
        runtimeInit.produce(new RuntimeInitializedClassBuildItem(
                "io.lettuce.core.metrics.DefaultCommandLatencyCollector"));
        runtimeInit.produce(new RuntimeInitializedClassBuildItem(
                "io.lettuce.core.metrics.DefaultCommandLatencyCollector$DefaultPauseDetectorWrapper"));
        runtimeInit.produce(new RuntimeInitializedClassBuildItem(
                "io.lettuce.core.metrics.DefaultCommandLatencyCollector$NoOpPauseDetectorWrapper"));
        // RedisClient.create() is folded at build time, which transitively materializes a
        // DefaultClientResources.Builder holding a DnsAddressResolverGroup (whose DnsNameResolverBuilder
        // is runtime-init by default). Defer the factory and its supporting classes to runtime init.
        runtimeInit.produce(new RuntimeInitializedClassBuildItem("io.lettuce.core.RedisClient"));
        runtimeInit.produce(new RuntimeInitializedClassBuildItem("io.lettuce.core.resource.DefaultClientResources"));
        runtimeInit.produce(new RuntimeInitializedClassBuildItem("io.lettuce.core.resource.DefaultClientResources$Builder"));
        // AddressResolverGroupProvider and its inner DefaultDnsAddressResolverGroupWrapper hold a static
        // DnsAddressResolverGroup whose dnsResolverBuilder references io.netty.resolver.dns.DnsNameResolverBuilder
        // (runtime-init by default).
        runtimeInit.produce(new RuntimeInitializedClassBuildItem("io.lettuce.core.resource.AddressResolverGroupProvider"));
        runtimeInit.produce(new RuntimeInitializedClassBuildItem(
                "io.lettuce.core.resource.AddressResolverGroupProvider$DefaultDnsAddressResolverGroupWrapper"));
    }

    @BuildStep
    public void detectLettuceUsage(BuildProducer<RequestedLettuceClientBuildItem> requestLettuce,
            BeanDiscoveryFinishedBuildItem beans) {

        Set<String> names = new HashSet<>();

        for (InjectionPointInfo ip : beans.getInjectionPoints()) {
            if (!LETTUCE_INJECTION_TYPES.contains(ip.getRequiredType().name())) {
                continue;
            }
            AnnotationInstance clientName = ip.getRequiredQualifier(REDIS_CLIENT_ANNOTATION);
            if (clientName != null) {
                names.add(clientName.value().asString());
            } else if (ip.hasDefaultedQualifier()) {
                names.add(DEFAULT_CLIENT_NAME);
            }
        }

        // Produce only the Lettuce request item. We deliberately do NOT produce a
        // RequestedRedisClientBuildItem here: that item drives the Vert.x processors, which would
        // otherwise create a parallel Vert.x Redis client (and a Vert.x-backed RedisDataSource) for
        // every Lettuce injection. The Lettuce backend owns its own beans.
        for (String name : names) {
            requestLettuce.produce(new RequestedLettuceClientBuildItem(name));
        }
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    public void init(LettuceRecorder recorder,
            List<RequestedLettuceClientBuildItem> clients,
            ShutdownContextBuildItem shutdown,
            BuildProducer<SyntheticBeanBuildItem> syntheticBeans,
            VertxBuildItem vertxBuildItem) {

        if (clients.isEmpty()) {
            return;
        }

        Set<String> names = new HashSet<>();
        for (RequestedLettuceClientBuildItem client : clients) {
            names.add(client.name);
        }

        // Initialize shared resources and per-client factories
        recorder.initialize(vertxBuildItem.getVertx(), names);

        // Register synthetic beans per client
        for (String name : names) {
            Supplier<ActiveResult> checkActive = recorder.checkActive(name);

            syntheticBeans.produce(
                    createLettuceBean(name, LETTUCE_REDIS_CLIENT, ClassType.create(LETTUCE_REDIS_CLIENT),
                            checkActive, recorder.getRedisClient(name)));
            syntheticBeans.produce(
                    createLettuceBean(name, LETTUCE_STATEFUL_CONNECTION, STATEFUL_CONNECTION_STRING_STRING,
                            checkActive, recorder.getConnection(name)));
        }

        // Shared ClientResources bean (singleton, default qualifier)
        syntheticBeans.produce(createLettuceBean(DEFAULT_CLIENT_NAME, LETTUCE_CLIENT_RESOURCES,
                ClassType.create(LETTUCE_CLIENT_RESOURCES),
                recorder.checkActive(DEFAULT_CLIENT_NAME), recorder.getClientResources()));

        // Register shutdown in correct order: connections → clients → resources
        recorder.cleanup(shutdown);
    }

    /**
     * Creates a Lettuce synthetic bean with the given type, checkActive guard, and supplier.
     */
    static SyntheticBeanBuildItem createLettuceBean(String name, DotName implClass, Type beanType,
            Supplier<ActiveResult> checkActive, Supplier<?> supplier) {

        SyntheticBeanBuildItem.ExtendedBeanConfigurator configurator = SyntheticBeanBuildItem
                .configure(implClass)
                .addType(beanType)
                .checkActive(checkActive)
                .startup()
                .setRuntimeInit()
                .unremovable()
                .supplier(supplier)
                .scope(ApplicationScoped.class);

        if (DEFAULT_CLIENT_NAME.equalsIgnoreCase(name)) {
            configurator.addQualifier(Default.class);
        } else {
            configurator.addQualifier().annotation(REDIS_CLIENT_ANNOTATION).addValue("value", name).done();
        }

        return configurator.done();
    }
}
