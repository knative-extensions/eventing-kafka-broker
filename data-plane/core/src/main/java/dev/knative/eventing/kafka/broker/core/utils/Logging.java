package dev.knative.eventing.kafka.broker.core.utils;

import java.util.function.BiFunction;

public class Logging {

  private static final BiFunction<String, Object, Object> keyValueLoaded = loadKeyValue();

  /**
   * Try to load key value class, otherwise if class not found then fallback to a string appender
   */
  private static BiFunction<String, Object, Object> loadKeyValue() {
    try {
      return net.logstash.logback.argument.StructuredArguments::keyValue;
    } catch (Throwable ignore) { // Class not found!
      return (k, v) -> k + "=" + v;
    }
  }

  public static Object keyValue(String key, Object value) {
    return keyValueLoaded.apply(key, value);
  }

}
