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

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Vert.x Kafka consumer records
 */
public interface KafkaConsumerRecords<K, V> {
  /**
   * @return the total number of records in this batch
   */
  int size();
  /**
   * @return whether this batch contains any records
   */
  boolean isEmpty();
  /**
   * Get the record at the given index
   * @param index the index of the record to get
   * @throws IndexOutOfBoundsException if index <0 or index>={@link #size()}
   */
  KafkaConsumerRecord<K, V> recordAt(int index);

  /**
   * @return  the native Kafka consumer records with backed information
   */
  ConsumerRecords<K, V> records();

}
