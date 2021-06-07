package dev.knative.eventing.kafka.broker.core.filter.impl;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

public class AllFilterTest {

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
  public void match(CloudEvent event, Filter filter, boolean shouldMatch) {
    assertThat(filter.test(event))
      .isEqualTo(shouldMatch);
  }

  static Stream<Arguments> testCases() {
    return Stream.of(
      Arguments.of(event, new AllFilter(Set.of(new ExactFilter("id", "123-42"))), true),
      Arguments.of(event, new AllFilter(Set.of(new ExactFilter("id", "123-42"), new ExactFilter("source", "/api/some-source"))), true),
      Arguments.of(event, new AllFilter(Set.of(new ExactFilter("id", "123"), new ExactFilter("source", "/api/some-source"))), false),
      Arguments.of(event, new AllFilter(Set.of(new ExactFilter("id", "123-42"), new ExactFilter("source", "/api/something-else"))), false),
      Arguments.of(event, new AllFilter(Collections.emptySet()), true)
    );
  }

}
