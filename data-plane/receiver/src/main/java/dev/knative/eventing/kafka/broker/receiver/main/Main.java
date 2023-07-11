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
package dev.knative.eventing.kafka.broker.receiver.main;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

import dev.knative.eventing.kafka.broker.core.ReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig;
import dev.knative.eventing.kafka.broker.core.utils.Configurations;
import dev.knative.eventing.kafka.broker.core.utils.Shutdown;
import io.cloudevents.kafka.CloudEventSerializer;
import io.cloudevents.kafka.PartitionKeyExtensionInterceptor;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    static {
        System.setProperty("logback.configurationFile", "/etc/logging/config.xml");
    }

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Start receiver.
     *
     * @param args command line arguments.
     */
    public static void start(final String[] args, final ReactiveProducerFactory kafkaProducerFactory)
            throws IOException {
        ReceiverEnv env = new ReceiverEnv(System::getenv);

        OpenTelemetrySdk openTelemetry =
                TracingConfig.fromDir(env.getConfigTracingPath()).setup();

        // Read producer properties and override some defaults
        Properties producerConfigs = Configurations.readPropertiesSync(env.getProducerConfigFilePath());
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
        producerConfigs.put(
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());

        logger.info("Starting Receiver {}", keyValue("env", env));

        // Start Vertx
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setMetricsOptions(Metrics.getOptions(env))
                .setTracingOptions(new OpenTelemetryOptions(openTelemetry)));

        // Register Contract message codec
        ContractMessageCodec.register(vertx.eventBus());

        // Read http server configuration and merge it with port from env
        HttpServerOptions httpServerOptions =
                new HttpServerOptions(Configurations.readPropertiesAsJsonSync(env.getHttpServerConfigFilePath()));
        httpServerOptions.setPort(env.getIngressPort());
        httpServerOptions.setTracingPolicy(TracingPolicy.PROPAGATE);

        // Read https server configuration and merge it with port from env
        HttpServerOptions httpsServerOptions =
                new HttpServerOptions(Configurations.readPropertiesAsJsonSync(env.getHttpServerConfigFilePath()));

        // Set the TLS port to a different port so that they don't have conflicts
        httpsServerOptions.setPort(env.getIngressTLSPort());
        httpsServerOptions.setTracingPolicy(TracingPolicy.PROPAGATE);

        // Configure the verticle to deploy and the deployment options
        final Supplier<Verticle> receiverVerticleFactory = new ReceiverVerticleFactory(
                env,
                producerConfigs,
                Metrics.getRegistry(),
                httpServerOptions,
                httpsServerOptions,
                kafkaProducerFactory);
        DeploymentOptions deploymentOptions =
                new DeploymentOptions().setInstances(Runtime.getRuntime().availableProcessors());

        try {
            // Deploy the receiver verticles
            vertx.deployVerticle(receiverVerticleFactory, deploymentOptions)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(env.getWaitStartupSeconds(), TimeUnit.SECONDS);
            logger.info("Receiver started");

            ContractPublisher publisher =
                    new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);
            FileWatcher fileWatcher = new FileWatcher(new File(env.getDataPlaneConfigFilePath()), publisher);
            fileWatcher.start();

            // Register shutdown hook for graceful shutdown.
            Shutdown.registerHook(vertx, publisher, fileWatcher, openTelemetry.getSdkTracerProvider());

        } catch (final Exception ex) {
            logger.error("Failed to startup the receiver", ex);
            Shutdown.closeVertxSync(vertx);
            System.exit(1);
        }
    }
}
