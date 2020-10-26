/*
 * Copyright 2020 The Knative Authors
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

package dev.knative.eventing.kafka.broker.dispatcher;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.metrics.MetricsOptionsProvider;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.utils.Configurations;
import dev.knative.eventing.kafka.broker.dispatcher.http.HttpConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import net.logstash.logback.encoder.LogstashEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  // Micrometer employs a naming convention that separates lowercase words with a '.' (dot) character.
  // Different monitoring systems have different recommendations regarding naming convention, and some naming
  // conventions may be incompatible for one system and not another.
  // Each Micrometer implementation for a monitoring system comes with a naming convention that transforms lowercase
  // dot notation names to the monitoring system’s recommended naming convention.
  // Additionally, this naming convention implementation sanitizes metric names and tags of special characters that
  // are disallowed by the monitoring system.
  public static final String HTTP_EVENTS_SENT_COUNT = "http.events.sent"; // prometheus format --> http_events_sent_total
  public static final String METRICS_REGISTRY_NAME = "metrics";

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  /**
   * Dispatcher entry point.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) {
    // HACK HACK HACK
    // maven-shade-plugin doesn't include the LogstashEncoder class, neither by specifying the
    // dependency with scope `provided` nor `runtime`, and adding include rules to
    // maven-shade-plugin.
    // Instantiating an Encoder here we force it to include the class.
    new LogstashEncoder().getFieldNames();

    final var env = new DispatcherEnv(System::getenv);

    logger.info("Starting Dispatcher {}", keyValue("env", env));

    final var vertx = Vertx.vertx(
      new VertxOptions().setMetricsOptions(MetricsOptionsProvider.get(env, METRICS_REGISTRY_NAME))
    );
    ContractMessageCodec.register(vertx.eventBus());

    final var metricsRegistry = BackendRegistries.getNow(METRICS_REGISTRY_NAME);
    final var eventsSentCounter = metricsRegistry.counter(HTTP_EVENTS_SENT_COUNT);

    Runtime.getRuntime().addShutdownHook(new Thread(vertx::close));

    final var producerConfig = Configurations.getProperties(env.getProducerConfigFilePath());
    final var consumerConfig = Configurations.getProperties(env.getConsumerConfigFilePath());
    final var webClientConfig = Configurations.getPropertiesAsJson(env.getWebClientConfigFilePath());

    final ConsumerRecordOffsetStrategyFactory<String, CloudEvent>
      consumerRecordOffsetStrategyFactory = ConsumerRecordOffsetStrategyFactory.unordered(eventsSentCounter);

    final var consumerVerticleFactory = new HttpConsumerVerticleFactory(
      consumerRecordOffsetStrategyFactory,
      consumerConfig,
      WebClient.create(vertx, new WebClientOptions(webClientConfig)),
      vertx,
      producerConfig
    );

    final var consumerDeployerVerticle = new ConsumerDeployer(
      consumerVerticleFactory,
      env.getEgressesInitialCapacity()
    );

    vertx.deployVerticle(consumerDeployerVerticle)
      .onSuccess(v -> logger.info("consumer deployer started"))
      .onFailure(t -> {
        // This is a catastrophic failure, close the application
        logger.error("consumer deployer not started", t);
        vertx.close(v -> System.exit(1));
      });

    try {
      final var fw = new FileWatcher(
        FileSystems.getDefault().newWatchService(),
        new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS),
        new File(env.getDataPlaneConfigFilePath())
      );

      fw.watch(); // block forever

    } catch (InterruptedException | IOException ex) {
      logger.error("failed during filesystem watch", ex);
    }
  }
}
