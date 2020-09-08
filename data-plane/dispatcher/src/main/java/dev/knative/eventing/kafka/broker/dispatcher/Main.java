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

import dev.knative.eventing.kafka.broker.core.ObjectsCreator;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.utils.Configurations;
import dev.knative.eventing.kafka.broker.dispatcher.http.HttpConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import net.logstash.logback.encoder.LogstashEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  /**
   * Dispatcher entry point.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) throws Exception {
    // HACK HACK HACK
    // maven-shade-plugin doesn't include the LogstashEncoder class, neither by specifying the
    // dependency with scope `provided` nor `runtime`, and adding include rules to
    // maven-shade-plugin.
    // Instantiating an Encoder here we force it to include the class.
    new LogstashEncoder().getFieldNames();

    DispatcherEnv env = new DispatcherEnv(System::getenv);

    logger.info("Starting Dispatcher");

    final var vertx = Vertx.vertx();
    Runtime.getRuntime().addShutdownHook(new Thread(vertx::close));

    final var producerConfig = Configurations.getProperties(env.getProducerConfigFilePath());
    final var consumerConfig = Configurations.getProperties(env.getConsumerConfigFilePath());
    final var webClientConfig = Configurations.getPropertiesAsJson(env.getWebClientConfigFilePath());

    final ConsumerRecordOffsetStrategyFactory<String, CloudEvent>
      consumerRecordOffsetStrategyFactory = ConsumerRecordOffsetStrategyFactory.unordered();

    final var consumerVerticleFactory = new HttpConsumerVerticleFactory(
      consumerRecordOffsetStrategyFactory,
      consumerConfig,
      WebClient.create(vertx, new WebClientOptions(webClientConfig)),
      vertx,
      producerConfig
    );

    final var brokersManager = new BrokersManager<>(
      vertx,
      consumerVerticleFactory,
      env.getBrokersInitialCapacity(),
      env.getTriggersInitialCapacity()
    );

    final var objectCreator = new ObjectsCreator(brokersManager);

    try {
      final var fw = new FileWatcher(
        FileSystems.getDefault().newWatchService(),
        objectCreator,
        new File(env.getDataPlaneConfigFilePath())
      );

      fw.watch(); // block forever

    } catch (InterruptedException | IOException ex) {
      logger.error("failed during filesystem watch", ex);
    }
  }

}
