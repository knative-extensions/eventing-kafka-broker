package dev.knative.eventing.kafka.broker.core.filter.impl;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

public class ExactFilterTest {

  final static CloudEvent event = CloudEventBuilder.v1()
    .withId("123-42")
    .withDataContentType("application/cloudevents+json")
    .withDataSchema(URI.create("/api/schema"))
    .withSource(URI.create("/api/some-source"))
    .withSubject("a-subject-42")
    .withType("type")
    .withTime(OffsetDateTime.of(
      1985, 4, 12,
      23, 20, 50, 0,
      ZoneOffset.UTC
    ))
    .build();

  @ParameterizedTest
  @MethodSource(value = {"testCases"})
  public void match(CloudEvent event, String key, String value, boolean shouldMatch) {
    var filter = new ExactFilter(key, value);
    assertThat(filter.test(event))
      .isEqualTo(shouldMatch);
  }

  static Stream<Arguments> testCases() {
    return Stream.of(
      Arguments.of(event, "id", "123-42", true),
      Arguments.of(event, "id", "123-43", false),
      Arguments.of(event, "source", "/api/some-source", true),
      Arguments.of(event, "source", "/api/something-else", false)
    );
  }

}
