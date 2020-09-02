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
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Properties;
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
  public static void main(final String[] args) {
    final var env = new Env(System::getenv);

    // HACK HACK HACK
    // maven-shade-plugin doesn't include the LogstashEncoder class, neither by specifying the
    // dependency with scope `provided` nor `runtime`, and adding include rules to
    // maven-shade-plugin.
    // Instantiating an Encoder here we force it to include the class.
    new LogstashEncoder().getFieldNames();

    logger.info("Starting Receiver {}", keyValue("env", env));

    final var producerConfigs = new Properties();
    try (final var configReader = new FileReader(env.getProducerConfigFilePath())) {
      producerConfigs.load(configReader);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    final var vertx = Vertx.vertx();
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    final var handler = new RequestHandler<>(
        producerConfigs,
        new CloudEventRequestToRecordMapper(),
        properties -> KafkaProducer.create(vertx, properties)
    );
    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(env.getIngressPort());
    final var verticle = new HttpVerticle(httpServerOptions, new SimpleProbeHandlerDecorator(
        env.getLivenessProbePath(), env.getReadinessProbePath(), handler
    ));

    vertx.deployVerticle(verticle)
        .onSuccess(v -> logger.info("receiver started"))
        .onFailure(t -> logger.error("receiver not started", t));

    try {
      final var fw = new FileWatcher(
          FileSystems.getDefault().newWatchService(),
          new ObjectsCreator(handler),
          new File(env.getDataPlaneConfigFilePath())
      );

      fw.watch(); // block forever

    } catch (InterruptedException | IOException ex) {
      logger.error("failed during filesystem watch", ex);
    }
  }
}
