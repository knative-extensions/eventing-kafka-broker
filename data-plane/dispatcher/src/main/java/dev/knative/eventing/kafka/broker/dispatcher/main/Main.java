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

import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig;
import dev.knative.eventing.kafka.broker.core.utils.Configurations;
import dev.knative.eventing.kafka.broker.core.utils.Shutdown;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEventInterceptor;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.KeyDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.cloudevents.kafka.PartitionKeyExtensionInterceptor;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  /**
   * Dispatcher entry point.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) throws IOException {
    DispatcherEnv env = new DispatcherEnv(System::getenv);

    OpenTelemetrySdk openTelemetry = TracingConfig.fromDir(env.getConfigTracingPath()).setup();

    // Read consumer and producer kafka config
    Properties producerConfig = Configurations.readPropertiesSync(env.getProducerConfigFilePath());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());
    producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());
    Properties consumerConfig = Configurations.readPropertiesSync(env.getConsumerConfigFilePath());
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KeyDeserializer.class.getName());
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());
    consumerConfig.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, InvalidCloudEventInterceptor.class.getName());

    // Read WebClient config
    JsonObject webClientConfig = Configurations.readPropertiesAsJsonSync(env.getWebClientConfigFilePath());
    WebClientOptions webClientOptions = new WebClientOptions(webClientConfig);
    webClientOptions.setTracingPolicy(TracingPolicy.PROPAGATE);

    logger.info("Configurations {} {} {}",
      keyValue("producerConfig", producerConfig),
      keyValue("consumerConfig", consumerConfig),
      keyValue("webClientConfig", webClientConfig)
    );
    logger.info("Starting Dispatcher {}", keyValue("env", env));

    // Start Vertx
    Vertx vertx = Vertx.vertx(
      new VertxOptions()
        .setMetricsOptions(Metrics.getOptions(env))
        .setTracingOptions(new OpenTelemetryOptions(openTelemetry))
    );

    // Register Contract message codec
    ContractMessageCodec.register(vertx.eventBus());

    try {
      // Create the consumer deployer
      ConsumerDeployerVerticle consumerDeployerVerticle = new ConsumerDeployerVerticle(
        new ConsumerVerticleFactoryImpl(
          consumerConfig,
          webClientOptions,
          producerConfig,
          AuthProvider.kubernetes(),
          Metrics.getRegistry()
        ),
        env.getEgressesInitialCapacity()
      );

      // Deploy the consumer deployer
      vertx.deployVerticle(consumerDeployerVerticle)
        .toCompletionStage()
        .toCompletableFuture()
        .get(env.getWaitStartupSeconds(), TimeUnit.SECONDS);

      logger.info("Consumer deployer started");

      ContractPublisher publisher = new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);
      FileWatcher fileWatcher = new FileWatcher(new File(env.getDataPlaneConfigFilePath()), publisher);
      fileWatcher.start();

      // Register shutdown hook for graceful shutdown.
      Shutdown.registerHook(vertx, publisher, fileWatcher, openTelemetry.getSdkTracerProvider());

    } catch (final Exception ex) {
      logger.error("Failed to startup the dispatcher", ex);
      Shutdown.closeVertxSync(vertx);
      System.exit(1);
    }
  }
}
