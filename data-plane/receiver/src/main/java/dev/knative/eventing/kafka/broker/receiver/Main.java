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

package dev.knative.eventing.kafka.broker.receiver;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.metrics.MetricsOptionsProvider;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.utils.Configurations;
import dev.knative.eventing.kafka.broker.core.utils.Shutdown;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.micrometer.backends.BackendRegistries;
import java.io.File;
import java.nio.file.FileSystems;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import net.logstash.logback.encoder.LogstashEncoder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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
  public static final String HTTP_REQUESTS_MALFORMED_COUNT = "http.requests.malformed"; // prometheus format --> http_requests_malformed_total
  public static final String HTTP_REQUESTS_PRODUCE_COUNT = "http.requests.produce";     // prometheus format --> http_requests_produce_total
  public static final String METRICS_REGISTRY_NAME = "metrics";

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  /**
   * Start receiver.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) {
    final var env = new ReceiverEnv(System::getenv);

    // HACK HACK HACK
    // maven-shade-plugin doesn't include the LogstashEncoder class, neither by specifying the
    // dependency with scope `provided` nor `runtime`, and adding include rules to
    // maven-shade-plugin.
    // Instantiating an Encoder here we force it to include the class.
    new LogstashEncoder().getFieldNames();

    final var producerConfigs = Configurations.getProperties(env.getProducerConfigFilePath());

    logger.info("Starting Receiver {}", keyValue("env", env));

    final var vertx = Vertx.vertx(
      new VertxOptions().setMetricsOptions(MetricsOptionsProvider.get(env, METRICS_REGISTRY_NAME))
    );

    try {

      ContractMessageCodec.register(vertx.eventBus());

      final var metricsRegistry = BackendRegistries.getNow(METRICS_REGISTRY_NAME);

      final var badRequestCounter = metricsRegistry.counter(HTTP_REQUESTS_MALFORMED_COUNT);
      final var produceEventsCounter = metricsRegistry.counter(HTTP_REQUESTS_PRODUCE_COUNT);

      producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
      producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      final var handler = new RequestMapper<>(
        producerConfigs,
        new CloudEventRequestToRecordMapper(),
        properties -> KafkaProducer.create(vertx, properties),
        badRequestCounter,
        produceEventsCounter
      );

      final var httpServerOptions = new HttpServerOptions(
        Configurations.getPropertiesAsJson(env.getHttpServerConfigFilePath())
      );
      httpServerOptions.setPort(env.getIngressPort());

      final var verticle = new ReceiverVerticle(
        httpServerOptions,
        handler,
        h -> new SimpleProbeHandlerDecorator(
          env.getLivenessProbePath(),
          env.getReadinessProbePath(),
          h
        )
      );

      final var waitVerticle = new CountDownLatch(1);
      vertx.deployVerticle(verticle)
        .onSuccess(v -> {
          logger.info("Receiver started");
          waitVerticle.countDown();
        })
        .onFailure(t -> {
          // This is a catastrophic failure, close the application
          logger.error("Consumer deployer not started", t);
          vertx.close(v -> System.exit(1));
        });
      waitVerticle.await(5, TimeUnit.SECONDS);

      final var publisher = new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);
      final var fs = FileSystems.getDefault().newWatchService();
      var fw = new FileWatcher(fs, publisher, new File(env.getDataPlaneConfigFilePath()));

      // Gracefully clean up resources.
      Runtime.getRuntime().addShutdownHook(new Thread(Shutdown.run(vertx, fw, publisher)));

      fw.watch(); // block forever

    } catch (final Exception ex) {
      logger.error("Failed during filesystem watch", ex);
    }

    Shutdown.closeSync(vertx).run();
  }
}
