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

/*
 * Copied from https://github.com/vert-x3/vertx-kafka-client
 *
 * Copyright 2016 Red Hat Inc.
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
package dev.knative.eventing.kafka.broker.vertx.kafka.producer.impl;

import dev.knative.eventing.kafka.broker.vertx.kafka.producer.KafkaHeader;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.KafkaProducerRecord;
import io.vertx.core.buffer.Buffer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Vert.x Kafka producer record implementation
 */
public class KafkaProducerRecordImpl<K, V> implements KafkaProducerRecord<K, V> {

  private final String topic;
  private final K key;
  private final V value;
  private final Long timestamp;
  private final Integer partition;
  private final List<KafkaHeader> headers = new ArrayList<>();

  /**
   * Constructor
   *
   * @param topic     the topic this record is being sent to
   * @param key       the key (or null if no key is specified)
   * @param value     the value
   * @param timestamp the timestamp of this record
   * @param partition the partition to which the record will be sent (or null if no partition was specified)
   */
  public KafkaProducerRecordImpl(String topic, K key, V value, Long timestamp, Integer partition) {

    this.topic = topic;
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
    this.partition = partition;
  }

  /**
   * Constructor
   *
   * @param topic     the topic this record is being sent to
   * @param key       the key (or null if no key is specified)
   * @param value     the value
   * @param partition the partition to which the record will be sent (or null if no partition was specified)
   */
  public KafkaProducerRecordImpl(String topic, K key, V value, Integer partition) {

    this.topic = topic;
    this.key = key;
    this.value = value;
    this.timestamp = null;
    this.partition = partition;
  }

  /**
   * Constructor
   *
   * @param topic the topic this record is being sent to
   * @param key   the key (or null if no key is specified)
   * @param value the value
   */
  public KafkaProducerRecordImpl(String topic, K key, V value) {

    this.topic = topic;
    this.key = key;
    this.value = value;
    this.timestamp = null;
    this.partition = null;
  }

  /**
   * Constructor
   *
   * @param topic the topic this record is being sent to
   * @param value the value
   */
  public KafkaProducerRecordImpl(String topic, V value) {

    this.topic = topic;
    this.key = null;
    this.value = value;
    this.timestamp = null;
    this.partition = null;
  }

  @Override
  public String topic() {
    return topic;
  }

  @Override
  public K key() {
    return key;
  }

  @Override
  public Long timestamp() {
    return timestamp;
  }

  @Override
  public V value() {
    return value;
  }

  @Override
  public Integer partition() {
    return partition;
  }

  @Override
  public KafkaProducerRecord<K, V> addHeader(String key, Buffer value) {
    return addHeader(new KafkaHeaderImpl(key, value));
  }

  @Override
  public KafkaProducerRecord<K, V> addHeader(String key, String value) {
    return addHeader(new KafkaHeaderImpl(key, value));
  }

  @Override
  public KafkaProducerRecord<K, V> addHeader(KafkaHeader header) {
    headers.add(header);
    return this;
  }

  @Override
  public KafkaProducerRecord<K, V> addHeaders(List<KafkaHeader> headers) {
    this.headers.addAll(headers);
    return this;
  }

  @Override
  public ProducerRecord<K, V> record() {
    if (headers.isEmpty()) {
      return new ProducerRecord<>(topic, partition, timestamp, key, value);
    } else {
      return new ProducerRecord<>(
        topic,
        partition,
        timestamp,
        key,
        value,
        headers.stream()
          .map(header -> new RecordHeader(header.key(), header.value().getBytes()))
          .collect(Collectors.toList()));
    }
  }

  @Override
  public List<KafkaHeader> headers() {
    return headers;
  }

  @Override
  public String toString() {

    return "KafkaProducerRecord{" +
      "topic=" + this.topic +
      ",partition=" + this.partition +
      ",timestamp=" + this.timestamp +
      ",key=" + this.key +
      ",value=" + this.value +
      ",headers=" + this.headers +
      "}";
  }
}
