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
import java.nio.file.ClosedWatchServiceException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import net.logstash.logback.encoder.LogstashEncoder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  /**
   * Start receiver.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) throws IOException {
    ReceiverEnv env = new ReceiverEnv(System::getenv);

    OpenTelemetrySdk openTelemetry = TracingConfig.fromDir(env.getConfigTracingPath()).setup();

    // HACK HACK HACK
    // maven-shade-plugin doesn't include the LogstashEncoder class, neither by specifying the
    // dependency with scope `provided` nor `runtime`, and adding include rules to
    // maven-shade-plugin.
    // Instantiating an Encoder here we force it to include the class.
    new LogstashEncoder().getFieldNames();

    // Read producer properties and override some defaults
    Properties producerConfigs = Configurations.readPropertiesSync(env.getProducerConfigFilePath());
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
    producerConfigs.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());

    logger.info("Starting Receiver {}", keyValue("env", env));

    // Start Vertx
    Vertx vertx = Vertx.vertx(
      new VertxOptions()
        .setMetricsOptions(Metrics.getOptions(env))
        .setTracingOptions(new OpenTelemetryOptions(openTelemetry))
    );

    // Register Contract message codec
    ContractMessageCodec.register(vertx.eventBus());

    // Read http server configuration and merge it with port from env
    HttpServerOptions httpServerOptions = new HttpServerOptions(
      Configurations.readPropertiesAsJsonSync(env.getHttpServerConfigFilePath())
    );
    httpServerOptions.setPort(env.getIngressPort());
    httpServerOptions.setTracingPolicy(TracingPolicy.PROPAGATE);

    // Configure the verticle to deploy and the deployment options
    final Supplier<Verticle> receiverVerticleFactory = new ReceiverVerticleFactory(
      env,
      producerConfigs,
      Metrics.getRegistry(),
      httpServerOptions
    );
    DeploymentOptions deploymentOptions = new DeploymentOptions()
      .setInstances(Runtime.getRuntime().availableProcessors());

    try {
      // Deploy the receiver verticles
      vertx.deployVerticle(receiverVerticleFactory, deploymentOptions)
        .toCompletionStage()
        .toCompletableFuture()
        .get(env.getWaitStartupSeconds(), TimeUnit.SECONDS);
      logger.info("Receiver started");

      final var publisher = new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);
      FileWatcher fileWatcher = new FileWatcher(new File(env.getDataPlaneConfigFilePath()), publisher);

      // Gracefully clean up resources.
      Shutdown.registerHook(vertx, publisher, fileWatcher, openTelemetry.getSdkTracerProvider());

    } catch (final ClosedWatchServiceException ignored) {
      // Do nothing, shutdown hook closed the watch service.
    } catch (final Exception ex) {
      logger.error("Failed to startup the receiver", ex);
      Shutdown.closeVertxSync(vertx);
      System.exit(1);
    }
  }
}
