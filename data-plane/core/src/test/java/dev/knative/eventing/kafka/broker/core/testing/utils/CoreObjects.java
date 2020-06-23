package dev.knative.eventing.kafka.broker.core.testing.utils;

import dev.knative.eventing.kafka.broker.core.BrokerWrapper;
import dev.knative.eventing.kafka.broker.core.TriggerWrapper;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Trigger;
import io.cloudevents.CloudEvent;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;

public final class CoreObjects {

  public static URL DESTINATION_URL;

  static {
    try {
      DESTINATION_URL = new URL(
          "http", "localhost", 44331, ""
      );
    } catch (final MalformedURLException e) {
      e.printStackTrace();
    }
  }

  public static final String DESTINATION = DESTINATION_URL.toString();

  public static Brokers brokers() {
    return Brokers.newBuilder()
        .addBroker(broker1Unwrapped())
        .addBroker(broker2Unwrapped())
        .build();
  }

  public static dev.knative.eventing.kafka.broker.core.Broker broker1() {
    return new BrokerWrapper(
        broker1Unwrapped()
    );
  }

  public static Broker broker1Unwrapped() {
    return Broker.newBuilder()
        .setDeadLetterSink(DESTINATION)
        .setId("1-1234")
        .setTopic("1-12345")
        .addAllTriggers(Arrays.asList(
            trigger11(),
            trigger12()
        ))
        .build();
  }

  public static dev.knative.eventing.kafka.broker.core.Broker broker2() {
    return new BrokerWrapper(
        broker2Unwrapped()
    );
  }

  public static Broker broker2Unwrapped() {
    return Broker.newBuilder()
        .setDeadLetterSink(DESTINATION)
        .setId("2-1234")
        .setTopic("2-12345")
        .addAllTriggers(Arrays.asList(
            trigger13(),
            trigger14()
        ))
        .build();
  }


  public static dev.knative.eventing.kafka.broker.core.Trigger<CloudEvent> trigger1() {
    return new TriggerWrapper(trigger11());
  }

  public static dev.knative.eventing.kafka.broker.core.Trigger<CloudEvent> trigger2() {
    return new TriggerWrapper(trigger12());
  }

  public static dev.knative.eventing.kafka.broker.core.Trigger<CloudEvent> trigger3() {
    return new TriggerWrapper(trigger13());
  }

  public static dev.knative.eventing.kafka.broker.core.Trigger<CloudEvent> trigger4() {
    return new TriggerWrapper(trigger14());
  }

  public static Trigger trigger11() {
    return Trigger.newBuilder()
        .setId("1-1234567")
        .setDestination(DESTINATION)
        .putAllAttributes(Map.of(
            "type", "dev.knative"
        ))
        .build();
  }

  public static Trigger trigger12() {
    return Trigger.newBuilder()
        .setId("2-1234567")
        .setDestination(DESTINATION)
        .putAllAttributes(Map.of(
            "type", "dev.knative"
        ))
        .build();
  }

  public static Trigger trigger13() {
    return Trigger.newBuilder()
        .setId("3-1234567")
        .setDestination(DESTINATION)
        .putAllAttributes(Map.of(
            "type", "dev.knative"
        ))
        .build();
  }

  public static Trigger trigger14() {
    return Trigger.newBuilder()
        .setId("4-1234567")
        .setDestination(DESTINATION)
        .putAllAttributes(Map.of(
            "type", "dev.knative"
        ))
        .build();
  }
}
