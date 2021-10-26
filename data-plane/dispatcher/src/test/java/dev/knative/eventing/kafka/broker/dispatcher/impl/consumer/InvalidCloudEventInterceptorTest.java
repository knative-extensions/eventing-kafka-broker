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
import io.cloudevents.core.data.BytesCloudEventData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class InvalidCloudEventInterceptorTest {


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

  @Test
  public void shouldBeDisabledIfNoKindPlural() {
    var interceptor = new InvalidCloudEventInterceptor();

    var configs = new HashMap<String, String>();
    withSourceName(configs);
    withSourceNamespace(configs);
    withEnabled(configs);

    interceptor.configure(configs);

    var want = mockValidRecords();

    var got = interceptor.onConsume(want);

    assertSame(want, got);
  }

  @Test
  public void shouldBeDisabledIfNoSourceName() {
    var interceptor = new InvalidCloudEventInterceptor();

    var configs = new HashMap<String, String>();
    withKindPlural(configs);
    withSourceNamespace(configs);
    withEnabled(configs);

    interceptor.configure(configs);

    var want = mockValidRecords();

    var got = interceptor.onConsume(want);

    assertSame(want, got);
  }

  @Test
  public void shouldBeDisabledIfNoSourceNamespace() {
    var interceptor = new InvalidCloudEventInterceptor();

    var configs = new HashMap<String, String>();
    withKindPlural(configs);
    withSourceName(configs);
    withEnabled(configs);

    interceptor.configure(configs);

    var records = mockValidRecords();

    var got = interceptor.onConsume(records);

    assertSame(records, got);
  }

  @Test
  public void shouldBeDisabled() {
    var interceptor = new InvalidCloudEventInterceptor();

    var configs = new HashMap<String, String>();
    withKindPlural(configs);
    withSourceName(configs);
    withSourceNamespace(configs);

    interceptor.configure(configs);

    var records = mockValidRecords();

    var got = interceptor.onConsume(records);

    assertSame(records, got);
  }

  @Test
  public void shouldDoNothingIfRecordsAreCloudEvents() {
    var interceptor = new InvalidCloudEventInterceptor();

    var configs = new HashMap<String, String>();
    withKindPlural(configs);
    withSourceName(configs);
    withSourceNamespace(configs);
    withEnabled(configs);

    interceptor.configure(configs);

    var want = mockValidRecords();

    var got = interceptor.onConsume(want);

    assertNotSame(want, got);

    for (final var r : want) {
      var tp = new TopicPartition(r.topic(), r.partition());
      assertEquals(want.records(tp), got.records(tp));
    }
  }

  @Test
  public void shouldTransformEventsToValidCloudEvents() {
    var interceptor = new InvalidCloudEventInterceptor();

    var configs = new HashMap<String, String>();
    withKindPlural(configs);
    withSourceName(configs);
    withSourceNamespace(configs);
    withEnabled(configs);

    interceptor.configure(configs);

    var input = mockInvalidRecords();

    var got = interceptor.onConsume(input);

    for (final var r : input) {
      var tp = new TopicPartition(r.topic(), r.partition());
      var inputRecords = input.records(tp);
      var expected = new ArrayList<ConsumerRecord<Object, CloudEvent>>();
      for (var i : inputRecords) {
        var value = CloudEventBuilder.v1()
          .withId(String.format("partition:%d/offset:%d", i.partition(), i.offset()))
          .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(i.timestamp()), ZoneId.of("UTC")))
          .withSource(URI.create(
            String.format("/apis/v1/namespaces/%s/%s/%s#%s",
              configs.get(InvalidCloudEventInterceptor.SOURCE_NAMESPACE_CONFIG),
              configs.get(InvalidCloudEventInterceptor.KIND_PLURAL_CONFIG),
              configs.get(InvalidCloudEventInterceptor.SOURCE_NAME_CONFIG),
              i.topic()
            )
          ))
          .withSubject(String.format("partition:%d#%d", i.partition(), i.offset()))
          .withType("dev.knative.kafka.event")
          .withData(BytesCloudEventData.wrap(new byte[]{1}));

        i.headers().forEach(h -> value.withExtension(h.key(), h.value()));

        setKeys(value, i);

        expected.add(new ConsumerRecord<>(
          i.topic(),
          i.partition(),
          i.offset(),
          i.timestamp(),
          i.timestampType(),
          i.checksum(),
          i.serializedKeySize(),
          i.serializedValueSize(),
          i.key(),
          value.build(),
          i.headers(),
          i.leaderEpoch()
        ));
      }

      var pairs = zip(expected, got.records(tp));
      for (var p : pairs) {
        assertConsumerRecordEquals(p.getKey(), p.getValue());
      }
    }
  }

  private void setKeys(io.cloudevents.core.v1.CloudEventBuilder value, ConsumerRecord<Object, CloudEvent> i) {
    if (i.key() instanceof Number) {
      value.withExtension("partitionkey", (Number) i.key());
      value.withExtension("key", (Number) i.key());
    } else if (i.key() instanceof String) {
      value.withExtension("partitionkey", i.key().toString());
      value.withExtension("key", i.key().toString());
    } else if (i.key() instanceof byte[]) {
      value.withExtension("partitionkey", (byte[]) i.key());
      value.withExtension("key", (byte[]) i.key());
    } else if (i.key() != null) {
      throw new IllegalArgumentException("unknown type for key: " + i.key());
    }
  }

  private void assertConsumerRecordEquals(final ConsumerRecord<Object, CloudEvent> actual,
                                          final ConsumerRecord<Object, CloudEvent> expected) {
    assertThat(actual.topic()).isEqualTo(expected.topic());
    assertThat(actual.partition()).isEqualTo(expected.partition());
    assertThat(actual.offset()).isEqualTo(expected.offset());
    assertThat(actual.key()).isEqualTo(expected.key());
    assertThat(actual.value()).isEqualTo(expected.value());
    assertThat(actual.serializedKeySize()).isEqualTo(expected.serializedKeySize());
    assertThat(actual.timestamp()).isEqualTo(expected.timestamp());
    assertThat(actual.timestampType()).isEqualTo(expected.timestampType());
    assertThat(actual.headers()).isEqualTo(expected.headers());
  }

  @Test
  public void shouldNotThrowExceptionDuringLifecycle() {
    var interceptor = new InvalidCloudEventInterceptor();

    var configs = new HashMap<String, String>();
    withKindPlural(configs);
    withSourceName(configs);
    withSourceNamespace(configs);
    withEnabled(configs);

    interceptor.configure(configs);

    var records = mockValidRecords();

    assertDoesNotThrow(() -> interceptor.onConsume(records));

    for (var r : records) {
      var tp = new TopicPartition(r.topic(), r.partition());
      assertDoesNotThrow(() -> interceptor.onCommit(Map.of(tp, new OffsetAndMetadata(r.offset()))));
    }

    assertDoesNotThrow(interceptor::close);
  }

  private static ConsumerRecords<Object, CloudEvent> mockValidRecords() {
    return new ConsumerRecords<>(Map.of(
      new TopicPartition("t1", 0),
      List.of(new ConsumerRecord<>("t1", 0, 0, "a", event))
    ));
  }

  private static ConsumerRecords<Object, CloudEvent> mockInvalidRecords() {
    return new ConsumerRecords<>(Map.of(
      new TopicPartition("t1", 0),
      List.of(
        new ConsumerRecord<>("t1", 0, 0, "a", new InvalidCloudEvent(new byte[]{1})),
        new ConsumerRecord<>("t1", 0, 1, "a", new InvalidCloudEvent(new byte[]{1})),
        new ConsumerRecord<>("t1", 0, 1, new byte[]{1, 2, 3, 4}, new InvalidCloudEvent(new byte[]{1})),
        new ConsumerRecord<>("t1", 0, 1, 4, new InvalidCloudEvent(new byte[]{1})),
        new ConsumerRecord<>("t1", 0, 1, null, new InvalidCloudEvent(new byte[]{1}))
      ),
      new TopicPartition("t1", 1),
      List.of(new ConsumerRecord<>(
        "t1", 1, 0, 1L, TimestampType.CREATE_TIME, 2L, 1, 2, "a",
        new InvalidCloudEvent(new byte[]{1}), new RecordHeaders().add(new RecordHeader("hello", "world".getBytes()))
      ))
    ));
  }


  private static void withKindPlural(final Map<String, String> configs) {
    with(configs, InvalidCloudEventInterceptor.KIND_PLURAL_CONFIG, "kafkasources");
  }

  private static void withSourceName(final Map<String, String> configs) {
    with(configs, InvalidCloudEventInterceptor.SOURCE_NAME_CONFIG, "ks");
  }

  private static void withSourceNamespace(final Map<String, String> configs) {
    with(configs, InvalidCloudEventInterceptor.SOURCE_NAMESPACE_CONFIG, "knative-ns");
  }

  private static void withEnabled(final Map<String, String> configs) {
    with(configs, CloudEventDeserializer.INVALID_CE_WRAPPER_ENABLED, "true");
  }

  private static <T> void with(final Map<String, T> configs, final String key, final T p) {
    configs.put(key, p);
  }

  public static <A, B> List<Map.Entry<A, B>> zip(List<A> as, List<B> bs) {
    if (as.size() != bs.size()) {
      throw new IllegalArgumentException("List must have the same length");
    }
    return IntStream.range(0, as.size())
      .mapToObj(i -> Map.entry(as.get(i), bs.get(i)))
      .collect(Collectors.toList());
  }
}
