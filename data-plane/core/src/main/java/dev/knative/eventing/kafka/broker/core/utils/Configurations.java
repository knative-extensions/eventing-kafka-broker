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
package dev.knative.eventing.kafka.broker.core.utils;

import io.vertx.core.json.JsonObject;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class Configurations {

  private static final Logger logger = LoggerFactory.getLogger(Configurations.class);

  /**
   * Retrieve a properties file.
   * <p>
   * Note: this method is blocking, thus it shouldn't be called on the event loop.
   */
  public static Properties readPropertiesSync(final String path) {
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
   * Retrieve a properties file and translates it to json.
   * <p>
   * Note: this method is blocking, thus it shouldn't be called on the event loop.
   */
  public static JsonObject readPropertiesAsJsonSync(final String path) {
    final var props = readPropertiesSync(path);

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
