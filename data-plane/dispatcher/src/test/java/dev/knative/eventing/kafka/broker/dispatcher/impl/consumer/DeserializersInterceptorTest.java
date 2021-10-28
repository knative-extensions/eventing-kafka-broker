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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventDeserializer.INVALID_CE_WRAPPER_ENABLED;
import static dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEventInterceptor.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DeserializersInterceptorTest {

  private static Stream<TestCase> testCases() {
    return Stream.of(
      // no CE, content-type application/json
      TestCase.builder()
        .setConfigs(Map.of(
          SOURCE_NAME_CONFIG, "ks",
          SOURCE_NAMESPACE_CONFIG, "ns",
          KIND_PLURAL_CONFIG, "kafkasources",
          INVALID_CE_WRAPPER_ENABLED, "true"
        ))
        .setHeaders(new RecordHeaders().add("content-type", "application/json".getBytes(StandardCharsets.UTF_8)))
        .setTopic("t1")
        .setKey("0".getBytes(StandardCharsets.UTF_8))
        .setExpectedKey("0")
        .setValue("{\"value\":5}".getBytes(StandardCharsets.UTF_8))
        .setExpectedEvent(CloudEventBuilder.v1()
          .withId("partition:%d/offset:%d".formatted(0, 0))
          .withType(TYPE)
          .withSource(URI.create("/apis/v1/namespaces/%s/kafkasources/%s#%s".formatted("ns", "ks", "t1")))
          .withDataContentType("application/json")
          .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")))
          .withSubject("partition:0#0")
          .withExtension("key", "0")
          .withExtension("partitionkey", "0")
          .withData("{\"value\":5}".getBytes(StandardCharsets.UTF_8))
          .build())
        .build(),
      // no CE, no content type header
      TestCase.builder()
        .setConfigs(Map.of(
          SOURCE_NAME_CONFIG, "ks",
          SOURCE_NAMESPACE_CONFIG, "ns",
          KIND_PLURAL_CONFIG, "kafkasources",
          INVALID_CE_WRAPPER_ENABLED, "true"
        ))
        .setTopic("t1")
        .setKey("0".getBytes(StandardCharsets.UTF_8))
        .setExpectedKey("0")
        .setValue("{\"value\":5}".getBytes(StandardCharsets.UTF_8))
        .setExpectedEvent(CloudEventBuilder.v1()
          .withId("partition:%d/offset:%d".formatted(0, 0))
          .withType(TYPE)
          .withSource(URI.create("/apis/v1/namespaces/%s/kafkasources/%s#%s".formatted("ns", "ks", "t1")))
          .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")))
          .withSubject("partition:0#0")
          .withExtension("key", "0")
          .withExtension("partitionkey", "0")
          .withData("{\"value\":5}".getBytes(StandardCharsets.UTF_8))
          .build())
        .build(),
      // no CE, no content type header, no key
      TestCase.builder()
        .setConfigs(Map.of(
          SOURCE_NAME_CONFIG, "ks",
          SOURCE_NAMESPACE_CONFIG, "ns",
          KIND_PLURAL_CONFIG, "kafkasources",
          INVALID_CE_WRAPPER_ENABLED, "true"
        ))
        .setTopic("t1")
        .setValue("{\"value\":5}".getBytes(StandardCharsets.UTF_8))
        .setExpectedEvent(CloudEventBuilder.v1()
          .withId("partition:%d/offset:%d".formatted(0, 0))
          .withType(TYPE)
          .withSource(URI.create("/apis/v1/namespaces/%s/kafkasources/%s#%s".formatted("ns", "ks", "t1")))
          .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")))
          .withSubject("partition:0#0")
          .withData("{\"value\":5}".getBytes(StandardCharsets.UTF_8))
          .build())
        .build(),
      // no CE, content-type text/plain
      TestCase.builder()
        .setConfigs(Map.of(
          SOURCE_NAME_CONFIG, "ks",
          SOURCE_NAMESPACE_CONFIG, "ns",
          KIND_PLURAL_CONFIG, "kafkasources",
          INVALID_CE_WRAPPER_ENABLED, "true"
        ))
        .setHeaders(new RecordHeaders().add("content-type", "text/plain".getBytes(StandardCharsets.UTF_8)))
        .setTopic("t1")
        .setKey("0".getBytes(StandardCharsets.UTF_8))
        .setExpectedKey("0")
        .setValue("simple 10".getBytes(StandardCharsets.UTF_8))
        .setExpectedEvent(CloudEventBuilder.v1()
          .withId("partition:%d/offset:%d".formatted(0, 0))
          .withType(TYPE)
          .withSource(URI.create("/apis/v1/namespaces/%s/kafkasources/%s#%s".formatted("ns", "ks", "t1")))
          .withDataContentType("text/plain")
          .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")))
          .withSubject("partition:0#0")
          .withExtension("key", "0")
          .withExtension("partitionkey", "0")
          .withData("simple 10".getBytes(StandardCharsets.UTF_8))
          .build())
        .build(),
      // no CE, with headers
      TestCase.builder()
        .setConfigs(Map.of(
          SOURCE_NAME_CONFIG, "ks",
          SOURCE_NAMESPACE_CONFIG, "ns",
          KIND_PLURAL_CONFIG, "kafkasources",
          INVALID_CE_WRAPPER_ENABLED, "true"
        ))
        .setHeaders(new RecordHeaders()
          .add("content-type", "text/plain".getBytes(StandardCharsets.UTF_8))
          .add("foo", "bar".getBytes(StandardCharsets.UTF_8))
        )
        .setTopic("t1")
        .setKey("0".getBytes(StandardCharsets.UTF_8))
        .setExpectedKey("0")
        .setValue("simple 10".getBytes(StandardCharsets.UTF_8))
        .setExpectedEvent(CloudEventBuilder.v1()
          .withId("partition:%d/offset:%d".formatted(0, 0))
          .withType(TYPE)
          .withSource(URI.create("/apis/v1/namespaces/%s/kafkasources/%s#%s".formatted("ns", "ks", "t1")))
          .withDataContentType("text/plain")
          .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneId.of("UTC")))
          .withSubject("partition:0#0")
          .withExtension("key", "0")
          .withExtension("partitionkey", "0")
          .withExtension("kafkaheaderfoo", "bar")
          .withData("simple 10".getBytes(StandardCharsets.UTF_8))
          .build())
        .build()
    );
  }

  @ParameterizedTest
  @MethodSource(value = {"testCases"})
  public void runTest(final TestCase tc) {
    final var keyDeserializer = new KeyDeserializer();
    final var cloudEventDeserializer = new CloudEventDeserializer();
    final var interceptor = new InvalidCloudEventInterceptor();

    // Configure components
    keyDeserializer.configure(tc.configs, true);
    cloudEventDeserializer.configure(tc.configs, false);
    interceptor.configure(tc.configs);

    final var pair = tc.keyValueUsing(keyDeserializer, cloudEventDeserializer);
    final var key = pair.getKey();
    final var value = pair.getValue();

    final var tp = new TopicPartition(tc.topic, 0);
    final var inputRecords = new ConsumerRecords<>(Map.of(
      tp, List.of(new ConsumerRecord<>(
        tc.topic,
        0,
        0,
        0,
        TimestampType.CREATE_TIME,
        0L,
        tc.key == null ? 0 : tc.key.length,
        tc.value == null ? 0 : tc.value.length,
        key,
        value,
        tc.headers == null ? new RecordHeaders() : tc.headers
      ))
    ));

    final var outputRecords = interceptor.onConsume(inputRecords).records(tp);

    assertThat(outputRecords.size()).isEqualTo(1);

    final var outputRecord = outputRecords.get(0);

    assertThat(outputRecord.key()).isEqualTo(tc.expectedKey);
    assertThat(outputRecord.value()).isEqualTo(tc.expectedEvent);
  }

  static class TestCase {

    final Map<String, ?> configs;
    final String topic;
    final Headers headers;

    final byte[] value;
    final byte[] key;

    final CloudEvent expectedEvent;
    final Object expectedKey;

    public TestCase(final Map<String, ?> configs,
                    final String topic,
                    final Headers headers,
                    final byte[] value,
                    final byte[] key,
                    final CloudEvent expectedEvent,
                    final Object expectedKey) {
      this.configs = configs;
      this.topic = topic;
      this.headers = headers;
      this.value = value;
      this.key = key;
      this.expectedEvent = expectedEvent;
      this.expectedKey = expectedKey;
    }

    public static TestCaseBuilder builder() {
      return TestCaseBuilder.builder();
    }

    Map.Entry<Object, CloudEvent> keyValueUsing(final KeyDeserializer keyDeserializer,
                                                final CloudEventDeserializer cloudEventDeserializer) {
      if (headers == null) {
        return new AbstractMap.SimpleImmutableEntry<>(
          keyDeserializer.deserialize(topic, key),
          cloudEventDeserializer.deserialize(topic, value)
        );
      }
      return new AbstractMap.SimpleImmutableEntry<>(
        keyDeserializer.deserialize(topic, headers, key),
        cloudEventDeserializer.deserialize(topic, headers, value)
      );
    }
  }

  public static class TestCaseBuilder {

    private Map<String, ?> configs;
    private String topic;
    private Headers headers;
    private byte[] value;
    private byte[] key;
    private CloudEvent expectedEvent;
    private Object expectedKey;

    public static TestCaseBuilder builder() {
      return new TestCaseBuilder();
    }

    public TestCaseBuilder setConfigs(Map<String, ?> configs) {
      this.configs = configs;
      return this;
    }

    public TestCaseBuilder setTopic(String topic) {
      this.topic = topic;
      return this;
    }

    public TestCaseBuilder setHeaders(Headers headers) {
      this.headers = headers;
      return this;
    }

    public TestCaseBuilder setValue(byte[] value) {
      this.value = value;
      return this;
    }

    public TestCaseBuilder setKey(byte[] key) {
      this.key = key;
      return this;
    }

    public TestCaseBuilder setExpectedEvent(CloudEvent expectedEvent) {
      this.expectedEvent = expectedEvent;
      return this;
    }

    public TestCaseBuilder setExpectedKey(Object expectedKey) {
      this.expectedKey = expectedKey;
      return this;
    }

    public DeserializersInterceptorTest.TestCase build() {
      return new DeserializersInterceptorTest.TestCase(configs, topic, headers, value, key, expectedEvent, expectedKey);
    }
  }
}
