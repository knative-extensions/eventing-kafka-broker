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
import dev.knative.eventing.kafka.broker.dispatcher.http.HttpConsumerVerticleFactory;
import io.cloudevents.CloudEvent;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final String BROKERS_TRIGGERS_PATH = "BROKERS_TRIGGERS_PATH";
  private static final String PRODUCER_CONFIG_FILE_PATH = "PRODUCER_CONFIG_FILE_PATH";
  private static final String CONSUMER_CONFIG_FILE_PATH = "CONSUMER_CONFIG_FILE_PATH";
  private static final String BROKERS_INITIAL_CAPACITY = "BROKERS_INITIAL_CAPACITY";
  private static final String TRIGGERS_INITIAL_CAPACITY = "TRIGGERS_INITIAL_CAPACITY";

  /**
   * Dispatcher entry point.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) {

    final var vertx = Vertx.vertx();
    Runtime.getRuntime().addShutdownHook(new Thread(vertx::close));

    final JsonObject json;
    try {
      json = getConfigurations(vertx);
    } catch (InterruptedException e) {
      System.exit(1);
      return;
    }

    final var producerConfigs = config(json.getString(PRODUCER_CONFIG_FILE_PATH));
    final var consumerConfigs = config(json.getString(CONSUMER_CONFIG_FILE_PATH));

    final ConsumerRecordOffsetStrategyFactory<String, CloudEvent>
        consumerRecordOffsetStrategyFactory = ConsumerRecordOffsetStrategyFactory.create();

    final var consumerVerticleFactory = new HttpConsumerVerticleFactory(
        consumerRecordOffsetStrategyFactory,
        consumerConfigs,
        vertx.createHttpClient(),
        vertx,
        producerConfigs
    );

    final var brokersManager = new BrokersManager<>(
        vertx,
        consumerVerticleFactory,
        Integer.parseInt(json.getString(BROKERS_INITIAL_CAPACITY)),
        Integer.parseInt(json.getString(TRIGGERS_INITIAL_CAPACITY))
    );

    final var objectCreator = new ObjectsCreator(brokersManager);

    try {
      final var fw = new FileWatcher(
          FileSystems.getDefault().newWatchService(),
          objectCreator,
          new File(json.getString(BROKERS_TRIGGERS_PATH))
      );

      fw.watch(); // block forever

    } catch (InterruptedException | IOException ex) {
      logger.error("failed during filesystem watch", ex);
    }
  }

  private static Properties config(final String path) {
    if (path == null) {
      return new Properties();
    }

    final var consumerConfigs = new Properties();
    try (final var configReader = new FileReader(path)) {
      consumerConfigs.load(configReader);
    } catch (IOException e) {
      logger.error("failed to load configurations from file {} - cause {}", path, e);
    }

    return consumerConfigs;
  }

  private static JsonObject getConfigurations(final Vertx vertx) throws InterruptedException {

    final var envConfigs = new ConfigStoreOptions()
        .setType("env")
        .setOptional(false)
        .setConfig(new JsonObject().put("raw-data", true));

    final var configRetrieverOptions = new ConfigRetrieverOptions()
        .addStore(envConfigs);

    final var configRetriever = ConfigRetriever.create(vertx, configRetrieverOptions);

    final var waitConfigs = new ArrayBlockingQueue<JsonObject>(1);
    Future.future(configRetriever::getConfig)
        .onSuccess(waitConfigs::add)
        .onFailure(cause -> {
          logger.error("failed to retrieve configurations", cause);
          vertx.close(ignored -> System.exit(1));
        });

    return waitConfigs.take();
  }
}
