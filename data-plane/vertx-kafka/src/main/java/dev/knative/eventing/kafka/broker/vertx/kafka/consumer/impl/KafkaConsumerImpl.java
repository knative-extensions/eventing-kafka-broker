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

import dev.knative.eventing.kafka.broker.vertx.kafka.common.TopicPartition;
import dev.knative.eventing.kafka.broker.vertx.kafka.common.impl.CloseHandler;
import dev.knative.eventing.kafka.broker.vertx.kafka.common.impl.Helper;
import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.KafkaConsumer;
import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.KafkaConsumerRecords;
import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.KafkaReadStream;
import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.OffsetAndMetadata;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * Vert.x Kafka consumer implementation
 */
public class KafkaConsumerImpl<K, V> implements KafkaConsumer<K, V> {

  private final KafkaReadStream<K, V> stream;
  private final CloseHandler closeHandler;

  public KafkaConsumerImpl(KafkaReadStream<K, V> stream) {
    this.stream = stream;
    this.closeHandler = new CloseHandler((timeout, ar) -> stream.close().onComplete(ar));
  }

  public synchronized KafkaConsumerImpl<K, V> registerCloseHook() {
    Context context = Vertx.currentContext();
    if (context == null) {
      return this;
    }
    closeHandler.registerCloseHook((ContextInternal) context);
    return this;
  }

  @Override
  public KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  public Future<Void> pause(Set<TopicPartition> topicPartitions) {
    return this.stream.pause(Helper.to(topicPartitions));
  }

  @Override
  public Future<Void> resume(Set<TopicPartition> topicPartitions) {
    return this.stream.resume(Helper.to(topicPartitions));
  }

  @Override
  public Future<Void> subscribe(Set<String> topics) {
    return this.stream.subscribe(topics);
  }

  @Override
  public KafkaConsumer<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler) {
    this.stream.partitionsRevokedHandler(Helper.adaptHandler(handler));
    return this;
  }

  @Override
  public KafkaConsumer<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler) {
    this.stream.partitionsAssignedHandler(Helper.adaptHandler(handler));
    return this;
  }

  @Override
  public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    return this.stream.commit(Helper.to(offsets)).map(Helper::from);
  }

  @Override
  public Future<Void> close() {
    Promise<Void> promise = Promise.promise();
    this.closeHandler.close(promise);
    return promise.future();
  }

  @Override
  public Consumer<K, V> unwrap() {
    return this.stream.unwrap();
  }

  @Override
  public Future<KafkaConsumerRecords<K, V>> poll(final Duration timeout) {
    return stream.poll(timeout)
      .map(KafkaConsumerRecordsImpl::new);
  }
}
