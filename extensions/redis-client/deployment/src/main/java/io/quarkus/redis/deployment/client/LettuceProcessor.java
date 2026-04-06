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
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ShutdownContextBuildItem;
import io.quarkus.redis.runtime.client.lettuce.LettuceRecorder;
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

    @BuildStep
    public void detectLettuceUsage(BuildProducer<RequestedRedisClientBuildItem> requestRedis,
            BuildProducer<RequestedLettuceClientBuildItem> requestLettuce,
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

        for (String name : names) {
            requestRedis.produce(new RequestedRedisClientBuildItem(name));
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
