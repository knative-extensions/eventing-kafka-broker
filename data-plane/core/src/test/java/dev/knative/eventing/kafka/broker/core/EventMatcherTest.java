/*
 * Copyright 2020 The Knative Authors
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

package dev.knative.eventing.kafka.broker.core;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.EventMatcher.Constants;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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

    assertThat(match).isEqualTo(shouldMatch);
  }

  @Test
  public void shouldConsiderEmptyStringAsAnyValue() {

    final var event = CloudEventBuilder.v1()
      .withId("123")
      .withType("type")
      .withSubject("")
      .withSource(URI.create("/api/source"))
      .build();

    final var attributes = Map.of(
      "source", "/api/source",
      "type", ""
    );

    final var eventMatcher = new EventMatcher(attributes);

    final boolean match = eventMatcher.match(event);

    assertThat(match).isTrue();
  }


  @Test
  public void shouldPassOnMatchingExtensions() {

    final var event = CloudEventBuilder.v1()
      .withId("123")
      .withType("type")
      .withSubject("")
      .withSource(URI.create("/api/source"))
      .withExtension("extension2", "valueExtension2")
      .build();

    final var attributes = Map.of(
      "source", "/api/source",
      "extension1", "",
      "extension2", "valueExtension2"
    );

    final var eventMatcher = new EventMatcher(attributes);

    final boolean match = eventMatcher.match(event);

    assertThat(match).isTrue();
  }

  @Test
  public void shouldNotPassOnNonMatchingExtensions() {

    final var event = CloudEventBuilder.v1()
      .withId("123")
      .withType("type")
      .withSubject("")
      .withSource(URI.create("/api/source"))
      .withExtension("extension2", "valueExtension2")
      .build();

    final var attributes = Map.of(
      "extension2", "valueExtension"
    );

    final var eventMatcher = new EventMatcher(attributes);

    final boolean match = eventMatcher.match(event);

    assertThat(match).isFalse();
  }

  @Test
  public void test() {

    final var event = CloudEventBuilder.v1()
      .withId("f6bc4296-014b-4e67-9880-96f1b6b5610b")
      .withSource(URI.create("http://source2.com"))
      .withType("type2")
      .withDataContentType("application/json")
      .withExtension("nonmatchingextname", "extval1")
      .build();

    final var attributes = Map.of(
      "extname1", "",
      "source", "",
      "type", ""
    );

    final var eventMatcher = new EventMatcher(attributes);

    final boolean match = eventMatcher.match(event);

    assertThat(match).isTrue();
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
          .withTime(OffsetDateTime.of(
            1985, 4, 12,
            23, 20, 50, 0,
            ZoneOffset.UTC
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
          .withTime(OffsetDateTime.of(
            1985, 4, 12,
            23, 20, 50, 0,
            ZoneOffset.UTC
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
          .withTime(OffsetDateTime.of(
            1985, 4, 12,
            23, 20, 50, 0,
            ZoneOffset.UTC
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
          .withTime(OffsetDateTime.of(
            1985, 4, 12,
            23, 20, 50, 0,
            ZoneOffset.UTC
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
    assertThat(EventMatcher.attributesMapper.size() + 1).isEqualTo(size);
  }
}
