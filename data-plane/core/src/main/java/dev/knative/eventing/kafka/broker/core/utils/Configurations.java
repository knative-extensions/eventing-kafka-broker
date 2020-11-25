package dev.knative.eventing.kafka.broker.core.utils;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import io.vertx.core.json.JsonObject;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configurations {

  private static final Logger logger = LoggerFactory.getLogger(Configurations.class);

  /**
   * Retrieve a properties file. Note: this method is blocking
   */
  public static Properties getProperties(final String path) {
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

  /**
   * Retrieve a properties file and translates it to json. Note: this method is blocking
   */
  public static JsonObject getPropertiesAsJson(final String path) {
    final var props = getProperties(path);

    final JsonObject json = new JsonObject();
    props.stringPropertyNames().forEach(name -> json.put(name, convert(props.getProperty(name))));
    return json;
  }

  private static Object convert(String value) {
    Objects.requireNonNull(value);

    Boolean bool = asBoolean(value);
    if (bool != null) {
      return bool;
    }

    Double integer = asNumber(value);
    if (integer != null) {
      return integer;
    }

    return value;
  }

  private static Double asNumber(String s) {
    try {
      return Double.parseDouble(s);
    } catch (NumberFormatException nfe) {
      return null;
    }
  }

  private static Boolean asBoolean(String s) {
    if (s.equalsIgnoreCase("true")) {
      return Boolean.TRUE;
    } else if (s.equalsIgnoreCase("false")) {
      return Boolean.FALSE;
    } else {
      return null;
    }
  }
}
