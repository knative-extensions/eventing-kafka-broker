package dev.knative.eventing.kafka.broker.core.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LoggingTest {

  @Test
  void testKeyValueLoading() {
    assertThat(Logging.keyValue("abc", 123))
      .isNotNull();
  }

  @Test
  void testKeyValueReturnValue() {
    // Check if logstash json encoder is included or not
    String nameOfExpectedClass;

    try {
      nameOfExpectedClass = ClassLoader.getSystemClassLoader()
        .loadClass("net.logstash.logback.marker.ObjectAppendingMarker")
        .getName();
    } catch (ClassNotFoundException e) {
      nameOfExpectedClass = String.class.getName();
    }

    System.out.println("Name of expected class: " + nameOfExpectedClass);

    assertThat(Logging.keyValue("abc", 123).getClass().getName())
      .isEqualTo(nameOfExpectedClass);
  }

}
