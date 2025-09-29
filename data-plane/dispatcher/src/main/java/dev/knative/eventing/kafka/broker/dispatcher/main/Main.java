/*
 * Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.knative.eventing.kafka.broker.dispatcher.main;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

import dev.knative.eventing.kafka.broker.core.ReactiveConsumerFactory;
import dev.knative.eventing.kafka.broker.core.ReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.eventtype.EventType;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeCreator;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeCreatorImpl;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeListerFactory;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.CeSqlRuntimeManager;
import dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi.KnVerifyCorrelationId;
import dev.knative.eventing.kafka.broker.core.observability.ObservabilityConfig;
import dev.knative.eventing.kafka.broker.core.observability.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.observability.tracing.TracingProvider;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.utils.Configurations;
import dev.knative.eventing.kafka.broker.core.utils.Shutdown;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEventInterceptor;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.KeyDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.NullCloudEventInterceptor;
import io.cloudevents.kafka.CloudEventSerializer;
import io.cloudevents.kafka.PartitionKeyExtensionInterceptor;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    static {
        if (System.getProperty("logback.configurationFile") == null
                || System.getProperty("logback.configurationFile").isEmpty()) {
            System.setProperty("logback.configurationFile", "/etc/logging/config.xml");
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Dispatcher entry point.
     *
     * @param args command line arguments.
     */
    public static void start(
            final String[] args,
            final ReactiveConsumerFactory reactiveConsumerFactory,
            final ReactiveProducerFactory reactiveProducerFactory)
            throws IOException {
        DispatcherEnv env = new DispatcherEnv(System::getenv);

        ObservabilityConfig observabilityConfig = ObservabilityConfig.fromDir(env.getConfigObservabilityPath());

        OpenTelemetrySdk openTelemetry = new TracingProvider(observabilityConfig.getTracingConfig()).setup();

        // Read consumer and producer kafka config
        Properties producerConfig = Configurations.readPropertiesSync(env.getProducerConfigFilePath());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());
        producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());
        Properties consumerConfig = Configurations.readPropertiesSync(env.getConsumerConfigFilePath());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KeyDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());
        consumerConfig.put(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                NullCloudEventInterceptor.class.getName() + "," + InvalidCloudEventInterceptor.class.getName());

        // Read WebClient config
        JsonObject webClientConfig = Configurations.readPropertiesAsJsonSync(env.getWebClientConfigFilePath());
        WebClientOptions webClientOptions = new WebClientOptions(webClientConfig);
        webClientOptions.setTracingPolicy(TracingPolicy.PROPAGATE);

        logger.info(
                "Configurations {} {} {}",
                keyValue("producerConfig", producerConfig),
                keyValue("consumerConfig", consumerConfig),
                keyValue("webClientConfig", webClientConfig));
        logger.info("Starting Dispatcher {}", keyValue("env", env));

        // Start Vertx
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setMetricsOptions(Metrics.getOptions(env, observabilityConfig))
                .setTracingOptions(new OpenTelemetryOptions(openTelemetry)));

        // Register Contract message codec
        ContractMessageCodec.register(vertx.eventBus());

        final var kubernetesClient = new KubernetesClientBuilder().build();
        final var eventTypeClient = kubernetesClient.resources(EventType.class);
        EventTypeCreator eventTypeCreator;
        try {
            eventTypeCreator = new EventTypeCreatorImpl(eventTypeClient, vertx);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        CeSqlRuntimeManager.getInstance()
                .registerFunction(new KnVerifyCorrelationId(vertx, kubernetesClient, env.getServiceNamespace()));

        final var eventTypeListerFactory = new EventTypeListerFactory(eventTypeClient);

        try {
            // Create the consumer deployer
            ConsumerDeployerVerticle consumerDeployerVerticle = new ConsumerDeployerVerticle(
                    new ConsumerVerticleFactoryImpl(
                            consumerConfig,
                            webClientOptions,
                            producerConfig,
                            AuthProvider.kubernetes(vertx),
                            Metrics.getRegistry(),
                            reactiveConsumerFactory,
                            reactiveProducerFactory,
                            eventTypeCreator,
                            eventTypeListerFactory),
                    env.getEgressesInitialCapacity());

            // Deploy the consumer deployer
            vertx.deployVerticle(consumerDeployerVerticle)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(env.getWaitStartupSeconds(), TimeUnit.SECONDS);

            logger.info("Consumer deployer started");

            ContractPublisher publisher =
                    new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);

            File file = new File(env.getDataPlaneConfigFilePath());
            FileWatcher fileWatcher = new FileWatcher(file, () -> publisher.updateContract(file));
            fileWatcher.start();

            //     Register shutdown hook for graceful shutdown.
            Shutdown.registerHook(
                    vertx, publisher, fileWatcher, eventTypeListerFactory, openTelemetry.getSdkTracerProvider());

            File healthFile = new File("/tmp/healthy");
            try {
                healthFile.createNewFile();
            } catch (Exception e) {
                logger.warn("Failed to create health file.", e);
            }

        } catch (final Exception ex) {
            logger.error("Failed to startup the dispatcher", ex);
            Shutdown.closeVertxSync(vertx);
            System.exit(1);
        }
    }
}
