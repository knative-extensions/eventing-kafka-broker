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

import dev.knative.eventing.kafka.broker.core.ObjectsCreator;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.utils.Configurations;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import net.logstash.logback.encoder.LogstashEncoder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  /**
   * Start receiver.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) throws Exception {
    final var env = new ReceiverEnv(System::getenv);

    // HACK HACK HACK
    // maven-shade-plugin doesn't include the LogstashEncoder class, neither by specifying the
    // dependency with scope `provided` nor `runtime`, and adding include rules to
    // maven-shade-plugin.
    // Instantiating an Encoder here we force it to include the class.
    new LogstashEncoder().getFieldNames();

    logger.info("Starting Receiver {}", keyValue("env", env));

    final var producerConfigs = Configurations.getProperties(env.getProducerConfigFilePath());

    final var vertx = Vertx.vertx();

    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    final var handler = new RequestHandler<>(
      producerConfigs,
      new CloudEventRequestToRecordMapper(),
      properties -> KafkaProducer.create(vertx, properties)
    );

    final var httpServerOptions = new HttpServerOptions(
      Configurations.getPropertiesAsJson(env.getHttpServerConfigFilePath())
    );
    httpServerOptions.setPort(env.getIngressPort());

    final var verticle = new HttpVerticle(httpServerOptions, new SimpleProbeHandlerDecorator(
      env.getLivenessProbePath(), env.getReadinessProbePath(), handler
    ));

    vertx.deployVerticle(verticle)
      .onSuccess(v -> logger.info("receiver started"))
      .onFailure(t -> logger.error("receiver not started", t));

    try {
      // TODO add a shutdown hook that calls objectsCreator.reconcile(Brokers.newBuilder().build()),
      //  so that producers flush their buffers.
      //  Note: reconcile(Brokers) isn't thread safe so we need to make sure to not stop the watcher
      //  from calling reconcile first

      final var objectsCreator = new ObjectsCreator(handler);
      final var fw = new FileWatcher(
        FileSystems.getDefault().newWatchService(),
        objectsCreator,
        new File(env.getDataPlaneConfigFilePath())
      );

      fw.watch(); // block forever

    } catch (InterruptedException | IOException ex) {
      logger.error("failed during filesystem watch", ex);
    }
  }
}
