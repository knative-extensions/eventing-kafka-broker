package dev.knative.eventing.kafka.broker.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.knative.eventing.kafka.broker.core.EventMatcher.Constants;
import io.cloudevents.CloudEvent;
import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class EventMatcherTest {

  @ParameterizedTest
  @MethodSource(value = {"testCases"})
  public void match(
      final Map<String, String> attributes,
      final CloudEvent event,
      final boolean shouldMatch) {

    final var matcher = new EventMatcher(attributes);

    final var match = matcher.match(event);

    assertEquals(shouldMatch, match);
  }

  public static Stream<Arguments> testCases() {
    return Stream.of(
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "1.0"
            ),
            new io.cloudevents.core.v1.CloudEventBuilder()
                .withId("1234")
                .withSource(URI.create("/source"))
                .withType("type")
                .build(),
            true
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "0.3"
            ),
            new io.cloudevents.core.v03.CloudEventBuilder()
                .withId("1234")
                .withSource(URI.create("/source"))
                .withType("type")
                .build(),
            true
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "0.3",
                Constants.ID, "123-42"
            ),
            new io.cloudevents.core.v03.CloudEventBuilder()
                .withId("123-42")
                .withSource(URI.create("/source"))
                .withType("type")
                .build(),
            true
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "0.3",
                Constants.ID, "123-42"
            ),
            new io.cloudevents.core.v03.CloudEventBuilder()
                .withId("123-423")
                .withSource(URI.create("/source"))
                .withType("type")
                .build(),
            false
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "0.3",
                Constants.ID, "123-42",
                Constants.DATA_CONTENT_TYPE, "application/cloudevents+json"
            ),
            new io.cloudevents.core.v03.CloudEventBuilder()
                .withId("123-42")
                .withSource(URI.create("/source"))
                .withType("type")
                .withDataContentType("application/cloudevents+json")
                .build(),
            true
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "1.0",
                Constants.ID, "123-42",
                Constants.DATA_CONTENT_TYPE, "application/cloudevents+json",
                Constants.DATA_SCHEMA, "/api/schema"
            ),
            new io.cloudevents.core.v1.CloudEventBuilder()
                .withId("123-42")
                .withDataContentType("application/cloudevents+json")
                .withDataSchema(URI.create("/api/schema"))
                .withSource(URI.create("/source"))
                .withType("type")
                .build(),
            true
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "1.0",
                Constants.ID, "123-42",
                Constants.DATA_CONTENT_TYPE, "application/cloudevents+json",
                Constants.DATA_SCHEMA, "/api/schema",
                Constants.SOURCE, "/api/some-source"
            ),
            new io.cloudevents.core.v1.CloudEventBuilder()
                .withId("123-42")
                .withDataContentType("application/cloudevents+json")
                .withDataSchema(URI.create("/api/schema"))
                .withSource(URI.create("/api/some-source"))
                .withType("type")
                .build(),
            true
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "1.0",
                Constants.ID, "123-42",
                Constants.DATA_CONTENT_TYPE, "application/cloudevents+json",
                Constants.SOURCE, "/api/schema",
                Constants.DATA_SCHEMA, "/api/some-source"
            ),
            new io.cloudevents.core.v1.CloudEventBuilder()
                .withId("123-42")
                .withDataContentType("application/cloudevents+json")
                .withDataSchema(URI.create("/api/schema"))
                .withSource(URI.create("/api/some-source"))
                .withType("type")
                .build(),
            false
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "1.0",
                Constants.ID, "123-42",
                Constants.DATA_CONTENT_TYPE, "application/cloudevents+json",
                Constants.DATA_SCHEMA, "/api/schema",
                Constants.SOURCE, "/api/some-source",
                Constants.SUBJECT, "a-subject-42",
                Constants.TIME, "1985-04-12T23:20:50Z",
                Constants.SCHEMA_URL, "/api/schema-url"
            ),
            new io.cloudevents.core.v1.CloudEventBuilder()
                .withId("123-42")
                .withDataContentType("application/cloudevents+json")
                .withDataSchema(URI.create("/api/schema"))
                .withSource(URI.create("/api/some-source"))
                .withSubject("a-subject-42")
                .withType("type")
                .withTime(ZonedDateTime.of(
                    1985, 4, 12,
                    23, 20, 50, 0,
                    ZoneId.of("Z")
                ))
                .build(),
            false
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "0.3",
                Constants.ID, "123-42",
                Constants.DATA_CONTENT_TYPE, "application/cloudevents+json",
                Constants.SOURCE, "/api/some-source",
                Constants.SUBJECT, "a-subject-42",
                Constants.TIME, "1985-04-12T23:20:50Z",
                Constants.SCHEMA_URL, "/api/schema-url"
            ),
            new io.cloudevents.core.v03.CloudEventBuilder()
                .withId("123-42")
                .withDataContentType("application/cloudevents+json")
                .withSchemaUrl(URI.create("/api/schema-url"))
                .withSource(URI.create("/api/some-source"))
                .withSubject("a-subject-42")
                .withType("type")
                .withTime(ZonedDateTime.of(
                    1985, 4, 12,
                    23, 20, 50, 0,
                    ZoneId.of("Z")
                ))
                .build(),
            true
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "0.3",
                Constants.ID, "123-42",
                Constants.DATA_CONTENT_TYPE, "application/cloudevents+json",
                Constants.SOURCE, "/api/some-source",
                Constants.SUBJECT, "a-subject-42",
                Constants.TIME, "1985-04-12T23:20:50Z",
                Constants.SCHEMA_URL, "/api/schema-url",
                Constants.TYPE, "dev.knative.eventing.create"
            ),
            new io.cloudevents.core.v03.CloudEventBuilder()
                .withId("123-42")
                .withDataContentType("application/cloudevents+json")
                .withSchemaUrl(URI.create("/api/schema-url"))
                .withSource(URI.create("/api/some-source"))
                .withSubject("a-subject-42")
                .withType("dev.knative.eventing.create")
                .withTime(ZonedDateTime.of(
                    1985, 4, 12,
                    23, 20, 50, 0,
                    ZoneId.of("Z")
                ))
                .build(),
            true
        ),
        Arguments.of(
            Map.of(
                Constants.SPEC_VERSION, "1.0",
                Constants.ID, "123-42",
                Constants.DATA_CONTENT_TYPE, "application/cloudevents+json",
                Constants.SOURCE, "/api/some-source",
                Constants.SUBJECT, "a-subject-42",
                Constants.TIME, "1985-04-12T23:20:50Z",
                Constants.DATA_SCHEMA, "/api/schema",
                Constants.TYPE, "dev.knative.eventing.create"
            ),
            new io.cloudevents.core.v1.CloudEventBuilder()
                .withId("123-42")
                .withDataContentType("application/cloudevents+json")
                .withDataSchema(URI.create("/api/schema"))
                .withSource(URI.create("/api/some-source"))
                .withSubject("a-subject-42")
                .withType("dev.knative.eventing.create")
                .withTime(ZonedDateTime.of(
                    1985, 4, 12,
                    23, 20, 50, 0,
                    ZoneId.of("Z")
                ))
                .build(),
            true
        )
    );
  }

  @Test
  public void shouldSetAllAttributes() {
    final var size = Stream.concat(
        io.cloudevents.core.v1.ContextAttributes.VALUES.stream(),
        io.cloudevents.core.v03.ContextAttributes.VALUES.stream()
    )
        .collect(Collectors.toSet())
        .size();

    // DATACONTENTENCODING isn't usable, so +1
    assertEquals(size, EventMatcher.attributesMapper.size() + 1);
  }
}