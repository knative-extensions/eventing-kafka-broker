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
package dev.knative.eventing.kafka.broker.vertx.kafka.consumer.impl;

import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.KafkaConsumerRecord;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.KafkaHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Vert.x Kafka consumer record implementation
 */
public class KafkaConsumerRecordImpl<K, V> implements KafkaConsumerRecord<K, V> {

  private final ConsumerRecord<K, V> record;
  private List<KafkaHeader> headers;

  /**
   * Constructor
   *
   * @param record  Kafka consumer record for backing information
   */
  public KafkaConsumerRecordImpl(ConsumerRecord<K, V> record) {
    this.record = record;
  }

  @Override
  public String topic() {
    return this.record.topic();
  }

  @Override
  public int partition() {
    return this.record.partition();
  }

  @Override
  public long offset() {
    return this.record.offset();
  }

  @Override
  public long timestamp() {
    return this.record.timestamp();
  }

  @Override
  public TimestampType timestampType() {
    return this.record.timestampType();
  }

  @Override
  public K key() {
    return this.record.key();
  }

  @Override
  public V value() {
    return this.record.value();
  }

  @Override
  public ConsumerRecord<K, V> record() {
    return this.record;
  }

  @Override
  public List<KafkaHeader> headers() {
    if (headers == null) {
      if (record.headers() == null) {
        headers = Collections.emptyList();
      } else {
        headers = new ArrayList<>();
        for (Header header : record.headers()) {
          headers.add(KafkaHeader.header(header.key(), header.value()));
        }
      }
    }
    return headers;
  }

  @Override
  public String toString() {

    return "KafkaConsumerRecord{" +
      "topic=" + this.record.topic() +
      ",partition=" + this.record.partition() +
      ",offset=" + this.record.offset() +
      ",timestamp=" + this.record.timestamp() +
      ",key=" + this.record.key() +
      ",value=" + this.record.value() +
      ",headers=" + this.record.headers() +
      "}";
  }
}
