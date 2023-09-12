package dev.knative.eventing.kafka.broker.dispatcher.impl.filter;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

import java.net.URI;

public class SampleEvent {
  public static CloudEvent event() {
    return CloudEventBuilder.v1()
      .withId("abcdefghijklmnop")
      .withSource(URI.create("http://localhost"))
      .withType("com.github.pull.create")
      .withDataSchema(URI.create("/api/schema"))
      .withDataContentType("testContentType")
      .withSubject("testSubject")
      .build();
  }
}
