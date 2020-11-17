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

package dev.knative.eventing.kafka.broker.core.tracing;

import io.opentelemetry.context.propagation.TextMapPropagator.Getter;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.LinkedList;
import java.util.stream.Collectors;

final class KafkaConsumerRecordGetter<K, V> implements Getter<KafkaConsumerRecord<K, V>> {

  @Override
  public Iterable<String> keys(final KafkaConsumerRecord<K, V> record) {
    if (record.headers() == null) {
      return new LinkedList<>();
    }

    return record.headers().stream()
      .map(KafkaHeader::key)
      .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public String get(final KafkaConsumerRecord<K, V> record, final String key) {
    if (record == null || record.headers() == null) {
      return null;
    }

    final var header = record.headers().stream()
      .filter(kafkaHeader -> kafkaHeader.key().equals(key))
      .findFirst();

    if (header.isEmpty()) {
      return null;
    }

    return header.get().value().toString();
  }
}
