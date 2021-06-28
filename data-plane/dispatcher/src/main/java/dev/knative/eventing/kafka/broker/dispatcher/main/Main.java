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
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerDeployerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.http.HttpConsumerVerticleFactory;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.cloudevents.kafka.PartitionKeyExtensionInterceptor;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import java.io.File;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.logstash.logback.encoder.LogstashEncoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class Main {

  // Micrometer employs a naming convention that separates lowercase words with a '.' (dot) character.
  // Different monitoring systems have different recommendations regarding naming convention, and some naming
  // conventions may be incompatible for one system and not another.
  // Each Micrometer implementation for a monitoring system comes with a naming convention that transforms lowercase
  // dot notation names to the monitoring system’s recommended naming convention.
  // Additionally, this naming convention implementation sanitizes metric names and tags of special characters that
  // are disallowed by the monitoring system.
  /**
   * In prometheus format --> http_events_sent_total
   */
  public static final String HTTP_EVENTS_SENT_COUNT = "http.events.sent";

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  /**
   * Dispatcher entry point.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) throws IOException {
    // HACK HACK HACK
    // maven-shade-plugin doesn't include the LogstashEncoder class, neither by specifying the
    // dependency with scope `provided` nor `runtime`, and adding include rules to
    // maven-shade-plugin.
    // Instantiating an Encoder here we force it to include the class.
    new LogstashEncoder().getFieldNames();

    final var env = new DispatcherEnv(System::getenv);

    final OpenTelemetrySdk openTelemetry = TracingConfig.fromDir(env.getConfigTracingPath()).setup();

    logger.info("Starting Dispatcher {}", keyValue("env", env));

    final var vertx = Vertx.vertx(
      new VertxOptions()
        .setMetricsOptions(Metrics.getOptions(env))
        .setTracingOptions(new OpenTelemetryOptions(openTelemetry))
    );

    try {

      ContractMessageCodec.register(vertx.eventBus());

      final var metricsRegistry = Metrics.getRegistry();
      final var eventsSentCounter = metricsRegistry.counter(HTTP_EVENTS_SENT_COUNT);

      final var producerConfig = Configurations.readPropertiesSync(env.getProducerConfigFilePath());
      producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());
      producerConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());
      final var consumerConfig = Configurations.readPropertiesSync(env.getConsumerConfigFilePath());
      consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());
      final var webClientConfig = Configurations.readPropertiesAsJsonSync(env.getWebClientConfigFilePath());

      logger.info("Configurations {} {} {}",
        keyValue("producerConfig", producerConfig),
        keyValue("consumerConfig", consumerConfig),
        keyValue("webClientConfig", webClientConfig)
      );

      final var clientOptions = new WebClientOptions(webClientConfig);
      clientOptions.setTracingPolicy(TracingPolicy.PROPAGATE);

      final var consumerVerticleFactory = new HttpConsumerVerticleFactory(
        consumerConfig,
        clientOptions,
        producerConfig,
        AuthProvider.kubernetes(),
        eventsSentCounter
      );

      final var consumerDeployerVerticle = new ConsumerDeployerVerticle(
        consumerVerticleFactory,
        env.getEgressesInitialCapacity()
      );

      final var waitConsumerDeployer = new CountDownLatch(1);
      vertx.deployVerticle(consumerDeployerVerticle)
        .onSuccess(v -> {
          logger.info("Consumer deployer started");
          waitConsumerDeployer.countDown();
        })
        .onFailure(t -> {
          // This is a catastrophic failure, close the application
          logger.error("Consumer deployer not started", t);
          vertx.close(v -> System.exit(1));
        });
      if (!waitConsumerDeployer.await(env.getWaitStartupSeconds(), TimeUnit.SECONDS)) {
        throw new TimeoutException("Failed to deploy consumer deployer");
      }

      final var publisher = new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);
      final var fs = FileSystems.getDefault().newWatchService();
      final var fw = new FileWatcher(fs, publisher, new File(env.getDataPlaneConfigFilePath()));

      // Gracefully clean up resources.
      Shutdown.registerHook(vertx, publisher, fw, openTelemetry.getSdkTracerProvider());

      fw.watch(); // block forever

    } catch (final ClosedWatchServiceException ignored) {
      // Do nothing, shutdown hook closed the watch service.
    } catch (final Exception ex) {
      logger.error("Failed during filesystem watch", ex);

      Shutdown.closeVertxSync(vertx);
      System.exit(1);
    }
  }
}
