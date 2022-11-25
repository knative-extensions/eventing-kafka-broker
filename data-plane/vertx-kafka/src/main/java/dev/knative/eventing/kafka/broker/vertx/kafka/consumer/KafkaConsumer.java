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

import dev.knative.eventing.kafka.broker.vertx.kafka.common.KafkaClientOptions;
import dev.knative.eventing.kafka.broker.vertx.kafka.common.TopicPartition;
import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.impl.KafkaConsumerImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * Vert.x Kafka consumer.
 * <p>
 * The {@link #pause(Set)} and {@link #resume(Set)} provides finer grained control over reading records
 * for specific Topic/Partition, these are Kafka's specific operations.
 */
public interface KafkaConsumer<K, V> {

  /**
   * Create a new KafkaConsumer instance from a native {@link Consumer}.
   *
   * @param vertx    Vert.x instance to use
   * @param consumer the Kafka consumer to wrap
   * @return an instance of the KafkaConsumer
   */
  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Consumer<K, V> consumer) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, consumer);
    return new KafkaConsumerImpl<>(stream);
  }

  /**
   * Create a new KafkaConsumer instance
   *
   * @param vertx   Vert.x instance to use
   * @param options Kafka consumer options
   * @return an instance of the KafkaConsumer
   */
  static <K, V> KafkaConsumer<K, V> create(Vertx vertx, KafkaClientOptions options) {
    KafkaReadStream<K, V> stream = KafkaReadStream.create(vertx, options);
    return new KafkaConsumerImpl<>(stream).registerCloseHook();
  }

  KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler);

  /**
   * Subscribe to the given list of topics to get dynamically assigned partitions.
   *
   * @param topics topics to subscribe to
   * @return a {@code Future} completed with the operation result
   */
  Future<Void> subscribe(Set<String> topics);

  /**
   * Suspend fetching from the requested partitions.
   *
   * @param topicPartitions topic partition from which suspend fetching
   * @return a {@code Future} completed with the operation result
   */
  Future<Void> pause(Set<TopicPartition> topicPartitions);

  /**
   * Resume specified partitions which have been paused with pause.
   *
   * @param topicPartitions topic partition from which resume fetching
   * @return a {@code Future} completed with the operation result
   */
  Future<Void> resume(Set<TopicPartition> topicPartitions);

  /**
   * Set the handler called when topic partitions are revoked to the consumer
   *
   * @param handler handler called on revoked topic partitions
   * @return current KafkaConsumer instance
   */
  KafkaConsumer<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler);

  /**
   * Set the handler called when topic partitions are assigned to the consumer
   *
   * @param handler handler called on assigned topic partitions
   * @return current KafkaConsumer instance
   */
  KafkaConsumer<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler);

  /**
   * Commit the specified offsets for the specified list of topics and partitions to Kafka.
   *
   * @param offsets offsets list to commit
   */
  Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offsets);

  /**
   * Close the consumer
   */
  Future<Void> close();

  /**
   * @return the underlying consumer
   */
  Consumer<K, V> unwrap();

  /**
   * Executes a poll for getting messages from Kafka.
   *
   * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
   * @return handler called after the poll with batch of records (can be empty).
   */
  Future<KafkaConsumerRecords<K, V>> poll(Duration timeout);

}
