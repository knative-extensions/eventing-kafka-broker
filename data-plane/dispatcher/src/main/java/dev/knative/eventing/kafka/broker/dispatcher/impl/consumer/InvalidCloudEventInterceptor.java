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
import io.cloudevents.core.v1.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;
import static io.cloudevents.kafka.PartitionKeyExtensionInterceptor.PARTITION_KEY_EXTENSION;

/**
 * The {@link InvalidCloudEventInterceptor} is a {@link ConsumerInterceptor}.
 * <p>
 * This interceptor is capable of building a valid {@link CloudEvent} from a message that doesn't follow the Kafka
 * protocol binding for CloudEvents.
 * <p>
 * The {@link CloudEventDeserializer} wraps invalid {@link CloudEvent}s in an instance of {@link InvalidCloudEvent}
 * so that this interceptor can build a valid {@link CloudEvent} from {@link ConsumerRecord}'s metadata and data.
 * <p>
 * For this reason this interceptor is capable of creating a CloudEvent from {@link ConsumerRecord} metadata and data.
 */
public class InvalidCloudEventInterceptor implements ConsumerInterceptor<Object, CloudEvent> {

  private static final Logger logger = LoggerFactory.getLogger(InvalidCloudEventInterceptor.class);

  public static final String KIND_PLURAL_CONFIG = "cloudevent.invalid.kind.plural";
  public static final String SOURCE_NAMESPACE_CONFIG = "cloudevent.invalid.source.namespace";
  public static final String SOURCE_NAME_CONFIG = "cloudevent.invalid.source.name";

  public static final String TYPE = "dev.knative.kafka.event";

  private static final String KEY_EXTENSION = "key";

  private String kindPlural;
  private String sourceNamespace;
  private String sourceName;

  private boolean isEnabled = false;

  @Override
  public void configure(final Map<String, ?> configs) {
    this.kindPlural = getOrDefault(configs, KIND_PLURAL_CONFIG);
    this.sourceName = getOrDefault(configs, SOURCE_NAME_CONFIG);
    this.sourceNamespace = getOrDefault(configs, SOURCE_NAMESPACE_CONFIG);

    if (configs.containsKey(CloudEventDeserializer.INVALID_CE_WRAPPER_ENABLED)) {
      final var enabled = configs.get(CloudEventDeserializer.INVALID_CE_WRAPPER_ENABLED);
      if (enabled != null) {
        isEnabled = Boolean.parseBoolean(enabled.toString());
      }
    }

    // Enable interceptor when all variables are defined.
    isEnabled = isEnabled &&
      isNotBlank(kindPlural) &&
      isNotBlank(sourceName) &&
      isNotBlank(sourceNamespace);

    logger.info("Interceptor config {} {} {} {}",
      keyValue("enabled", isEnabled),
      keyValue(KIND_PLURAL_CONFIG, kindPlural),
      keyValue(SOURCE_NAME_CONFIG, sourceName),
      keyValue(SOURCE_NAMESPACE_CONFIG, sourceNamespace)
    );
  }

  @Override
  public ConsumerRecords<Object, CloudEvent> onConsume(final ConsumerRecords<Object, CloudEvent> records) {
    if (!this.isEnabled || records == null || records.isEmpty()) {
      return records;
    }

    final Map<TopicPartition, List<ConsumerRecord<Object, CloudEvent>>> validRecords = new HashMap<>(records.count());
    for (final var record : records) {
      final var tp = new TopicPartition(record.topic(), record.partition());
      if (!validRecords.containsKey(tp)) {
        validRecords.put(tp, new ArrayList<>());
      }
      validRecords.get(tp).add(validRecord(record));
    }
    return new ConsumerRecords<>(validRecords);
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // Intentionally left blank.
  }

  @Override
  public void close() {
    // Intentionally left blank.
  }

  private static String getOrDefault(final Map<String, ?> configs, final String key) {
    if (configs.containsKey(key)) {
      return (String) configs.get(key);
    }
    return null;
  }

  private ConsumerRecord<Object, CloudEvent> validRecord(final ConsumerRecord<Object, CloudEvent> record) {
    if (!(record.value() instanceof InvalidCloudEvent)) {
      return record; // Valid CloudEvent
    }
    final var invalidEvent = (InvalidCloudEvent) record.value();
    // Handle invalid CloudEvents.
    // Create CloudEvent from the record metadata (topic, partition, offset, etc) and use received data as data field.
    // The record to CloudEvent conversion is compatible with eventing-kafka.
    final var value = new CloudEventBuilder()
      .withId(id(record))
      .withTime(time(record))
      .withType(type())
      .withSource(source(record))
      .withSubject(subject(record));

    setKey(value, record.key());

    if (invalidEvent.data() != null) {
      value.withData(invalidEvent.data());
    }

    // Put headers as event extensions.
    record.headers().forEach(header -> value.withExtension(replaceBadChars(header.key()), header.value()));

    // Copy consumer record and set value to a valid CloudEvent.
    return new ConsumerRecord<>(
      record.topic(),
      record.partition(),
      record.offset(),
      record.timestamp(),
      record.timestampType(),
      record.checksum(),
      record.serializedKeySize(),
      record.serializedValueSize(),
      record.key(),
      value.build(),
      record.headers(),
      record.leaderEpoch()
    );
  }

  private static void setKey(CloudEventBuilder value, final Object key) {
    if (key instanceof String) {
      final var keyCasted = (String) key;
      value.withExtension(PARTITION_KEY_EXTENSION, keyCasted);
      value.withExtension(KEY_EXTENSION, keyCasted);
    } else if (key instanceof Number) {
      final var keyCasted = (Number) key;
      value.withExtension(PARTITION_KEY_EXTENSION, keyCasted);
      value.withExtension(KEY_EXTENSION, keyCasted);
    } else if (key instanceof byte[]) {
      final var keyCasted = (byte[]) key;
      value.withExtension(PARTITION_KEY_EXTENSION, keyCasted);
      value.withExtension(KEY_EXTENSION, keyCasted);
    } else {
      throw new IllegalStateException("Unknown key type: " + key);
    }
  }

  private static String replaceBadChars(String value) {
    // Since we're doing casting, the final collector is valid for
    // any possible result type, assign the result of the pipeline
    // to a typed variable, so that we're sure that our variable is
    // actually a Stream of Characters.
    final Stream<Character> validChars = value.chars()
      // The CE spec allows only lower case english letters,
      // therefore transform to lower case so that we can preserve
      // as many characters as possible.
      .mapToObj(c -> Character.toLowerCase((char) c))
      .filter(InvalidCloudEventInterceptor::allowedChar);

    return validChars
      .collect(Collector.of(
        StringBuilder::new,
        StringBuilder::append,
        StringBuilder::append,
        StringBuilder::toString
      ));
  }

  private static boolean allowedChar(final char c) {
    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
  }

  private static String subject(final ConsumerRecord<Object, CloudEvent> record) {
    return "partition:" + record.partition() + "#" + record.offset();
  }

  private URI source(final ConsumerRecord<Object, CloudEvent> record) {
    return URI.create(
      "/apis/v1/namespaces/" +
        this.sourceNamespace +
        "/" +
        this.kindPlural +
        "/" +
        this.sourceName +
        "#" +
        record.topic()
    );
  }

  private static String type() {
    return TYPE;
  }

  private static OffsetDateTime time(final ConsumerRecord<Object, CloudEvent> record) {
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.of("UTC"));
  }

  private static String id(final ConsumerRecord<Object, CloudEvent> record) {
    return "partition:" + record.partition() + "/offset:" + record.offset();
  }

  private static boolean isNotBlank(final String s) {
    return s != null && !s.isBlank();
  }
}
