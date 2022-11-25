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
package dev.knative.eventing.kafka.broker.vertx.kafka.producer;

import dev.knative.eventing.kafka.broker.vertx.kafka.producer.impl.KafkaProducerImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

/**
 * Vert.x Kafka producer.
 */
public interface KafkaProducer<K, V> {

  /**
   * Create a new KafkaProducer instance from a native {@link Producer}.
   *
   * @param vertx    Vert.x instance to use
   * @param producer the Kafka producer to wrap
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Producer<K, V> producer) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, producer);
    return new KafkaProducerImpl<>(vertx, stream);
  }

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx  Vert.x instance to use
   * @param config Kafka producer configuration
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config);
    return new KafkaProducerImpl<>(vertx, stream).registerCloseHook();
  }

  KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler);

  /**
   * Asynchronously write a record to a topic
   *
   * @param record record to write
   * @return a {@code Future} completed with the record metadata
   */
  Future<RecordMetadata> send(KafkaProducerRecord<K, V> record);

  /**
   * Invoking this method makes all buffered records immediately available to write
   *
   * @return current KafkaProducer instance
   */
  Future<Void> flush();

  /**
   * Close the producer
   *
   * @return a {@code Future} completed with the operation result
   */
  Future<Void> close();

  /**
   * @return the underlying producer
   */
  Producer<K, V> unwrap();
}
