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
package dev.knative.eventing.kafka.broker.vertx.kafka.consumer;

import dev.knative.eventing.kafka.broker.vertx.kafka.producer.KafkaHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;

/**
 * Vert.x Kafka consumer record
 */
public interface KafkaConsumerRecord<K, V> {

  /**
   * @return the topic this record is received from
   */
  String topic();

  /**
   * @return the partition from which this record is received
   */
  int partition();

  /**
   * @return the position of this record in the corresponding Kafka partition.
   */
  long offset();

  /**
   * @return the timestamp of this record
   */
  long timestamp();

  /**
   * @return the timestamp type of this record
   */
  TimestampType timestampType();

  /**
   * @return the key (or null if no key is specified)
   */
  K key();

  /**
   * @return the value
   */
  V value();

  /**
   * @return the list of consumer record headers
   */
  List<KafkaHeader> headers();

  /**
   * @return the native Kafka consumer record with backed information
   */
  ConsumerRecord<K, V> record();
}
