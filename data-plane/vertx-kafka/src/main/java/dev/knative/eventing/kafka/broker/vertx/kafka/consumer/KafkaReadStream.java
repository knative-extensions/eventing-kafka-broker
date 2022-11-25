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
import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.impl.KafkaReadStreamImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A {@link ReadStream} for consuming Kafka {@link ConsumerRecord}.
 * <p>
 * The {@link #pause(Set)} and {@link #resume(Set)} provides finer grained control over reading records
 * for specific Topic/Partition, these are Kafka's specific operations.
 */
public interface KafkaReadStream<K, V> {

  KafkaReadStream<K, V> exceptionHandler(Handler<Throwable> handler);

  /**
   * Create a new KafkaReadStream instance
   *
   * @param vertx   Vert.x instance to use
   * @param options Kafka consumer options
   * @return an instance of the KafkaReadStream
   */
  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, KafkaClientOptions options) {
    Map<String, Object> config = new HashMap<>();
    if (options.getConfig() != null) {
      config.putAll(options.getConfig());
    }
    return new KafkaReadStreamImpl<>(vertx, new org.apache.kafka.clients.consumer.KafkaConsumer<>(config));
  }

  /**
   * Create a new KafkaReadStream instance
   *
   * @param vertx    Vert.x instance to use
   * @param consumer native Kafka consumer instance
   * @return an instance of the KafkaReadStream
   */
  static <K, V> KafkaReadStream<K, V> create(Vertx vertx, Consumer<K, V> consumer) {
    return new KafkaReadStreamImpl<>(vertx, consumer);
  }

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
   * @return current KafkaReadStream instance
   */
  KafkaReadStream<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler);

  /**
   * Set the handler called when topic partitions are assigned to the consumer
   *
   * @param handler handler called on assigned topic partitions
   * @return current KafkaReadStream instance
   */
  KafkaReadStream<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler);

  /**
   * Subscribe to the given list of topics to get dynamically assigned partitions.
   *
   * @param topics topics to subscribe to
   * @return a {@code Future} completed with the operation result
   */
  Future<Void> subscribe(Set<String> topics);

  /**
   * Commit the specified offsets for the specified list of topics and partitions to Kafka.
   *
   * @param offsets offsets list to commit
   * @return a {@code Future} completed with the operation result
   */
  Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offsets);

  /**
   * Close the stream
   *
   * @return a {@code Future} completed with the operation result
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
   */
  Future<ConsumerRecords<K, V>> poll(final Duration timeout);
}
