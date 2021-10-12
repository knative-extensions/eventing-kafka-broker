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

package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.KafkaMessageFactory;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CloudEventDeserializerTest {

  private final static CloudEvent event = CloudEventBuilder.v1()
    .withId("123-42")
    .withDataContentType("application/cloudevents+json")
    .withDataSchema(URI.create("/api/schema"))
    .withSource(URI.create("/api/some-source"))
    .withSubject("a-subject-42")
    .withType("type")
    .withData(new byte[]{1})
    .withTime(OffsetDateTime.of(
      1985, 4, 12,
      23, 20, 50, 0,
      ZoneOffset.UTC
    ))
    .build();

  @Test
  public void shouldDeserializeValidCloudEventBinary() {
    final var topic = "test";

    final var record = KafkaMessageFactory.createWriter(topic).writeBinary(event);

    final var deserializer = new CloudEventDeserializer();
    CloudEvent outEvent = deserializer.deserialize(topic, record.headers(), record.value());

    assertThat(outEvent).isEqualTo(event);
  }

  @Test
  public void shouldDeserializeValidCloudEventStructured() {
    final var topic = "test";

    final var record = KafkaMessageFactory.createWriter(topic).writeStructured(event, JsonFormat.CONTENT_TYPE);

    final var deserializer = new CloudEventDeserializer();
    CloudEvent outEvent = deserializer.deserialize(topic, record.headers(), record.value());

    assertThat(outEvent).isEqualTo(event);
  }

  @Test
  public void shouldDeserializeInvalidCloudEventWhenEnabled() {
    final var topic = "test";

    final var headers = new RecordHeaders()
      .add(new RecordHeader("knative", "knative".getBytes(StandardCharsets.UTF_8)));

    final var deserializer = new CloudEventDeserializer();
    final var configs = new HashMap<String, String>();
    configs.put(CloudEventDeserializer.INVALID_CE_WRAPPER_ENABLED, "true");
    deserializer.configure(configs, false);

    CloudEvent outEvent = deserializer.deserialize(
      topic,
      headers,
      new byte[]{1, 4}
    );

    assertThat(outEvent).isInstanceOf(InvalidCloudEvent.class);

    final var invalid = (InvalidCloudEvent) outEvent;

    assertThatThrownBy(invalid::getData);
    assertThatThrownBy(invalid::getDataContentType);
    assertThatThrownBy(invalid::getSource);
    assertThatThrownBy(invalid::getSubject);
    assertThatThrownBy(invalid::getId);
    assertThatThrownBy(invalid::getTime);
    assertThatThrownBy(invalid::getExtensionNames);
    assertThatThrownBy(() -> invalid.getExtension("a"));
    assertThatThrownBy(invalid::getDataSchema);
    assertThatThrownBy(invalid::getType);
    assertThatThrownBy(invalid::getSpecVersion);
    assertThatThrownBy(() -> invalid.getAttribute("type"));

    assertDoesNotThrow(invalid::data);
  }

  @Test
  public void shouldNotDeserializeInvalidCloudEventWhenDisabled() {
    final var topic = "test";

    final var headers = new RecordHeaders()
      .add(new RecordHeader("knative", "knative".getBytes(StandardCharsets.UTF_8)));

    final var deserializer = new CloudEventDeserializer();

    final var configs = new HashMap<String, String>();
    deserializer.configure(configs, false);

    assertThatThrownBy(() -> deserializer.deserialize(
      topic,
      headers,
      new byte[]{1, 4}
    ));
  }
}
