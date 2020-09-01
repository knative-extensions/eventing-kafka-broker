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

package dev.knative.eventing.kafka.broker.dispatcher;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Objects;

/**
 * ConsumerVerticle is responsible for manging the consumer lifecycle.
 *
 * @param <K> record key type.
 * @param <V> record value type.
 */
public final class ConsumerVerticle<K, V> extends AbstractVerticle {

  private final KafkaConsumer<K, V> consumer;
  private final String topic;
  private final Handler<KafkaConsumerRecord<K, V>> recordHandler;

  /**
   * All args constructor.
   *
   * @param consumer      Kafka consumer.
   * @param topic         topic to consume.
   * @param recordHandler handler of consumed Kafka records.
   */
  public ConsumerVerticle(
    final KafkaConsumer<K, V> consumer,
    final String topic,
    final Handler<KafkaConsumerRecord<K, V>> recordHandler) {

    Objects.requireNonNull(consumer, "provide consumer");
    Objects.requireNonNull(topic, "provide topic");
    Objects.requireNonNull(recordHandler, "provide record handler");

    this.recordHandler = recordHandler;
    this.consumer = consumer;
    this.topic = topic;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start(Promise<Void> startPromise) {
    consumer.handler(recordHandler);
    consumer.subscribe(topic, startPromise);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.close(stopPromise);
  }
}
