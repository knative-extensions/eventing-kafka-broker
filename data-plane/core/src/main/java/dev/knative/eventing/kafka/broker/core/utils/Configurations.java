package dev.knative.eventing.kafka.broker.core.utils;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configurations {

  private static final Logger logger = LoggerFactory.getLogger(Configurations.class);

  public static Properties getKafkaProperties(final String path) {
    if (path == null) {
      return new Properties();
    }

    final var props = new Properties();
    try (final var configReader = new FileReader(path)) {
      props.load(configReader);
    } catch (IOException e) {
      logger.error("failed to load configurations from file {}", keyValue("path", path), e);
    }

    return props;
  }

  public static JsonObject getFileConfigurations(final Vertx vertx, String file) throws ExecutionException, InterruptedException {
    final var fileConfigs = new ConfigStoreOptions()
      .setType("file")
      .setFormat("properties")
      .setConfig(new JsonObject().put("path", file));

    return ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(fileConfigs))
      .getConfig()
      .toCompletionStage()
      .toCompletableFuture()
      .get();
  }
}
