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

import dev.knative.eventing.kafka.broker.vertx.kafka.common.KafkaClientOptions;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.impl.KafkaWriteStreamImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * A {@link WriteStream} for writing to Kafka {@link ProducerRecord}.
 */
public interface KafkaWriteStream<K, V> {

  /**
   * Create a new KafkaWriteStream instance
   *
   * @param vertx  Vert.x instance to use
   * @param config Kafka producer configuration
   * @return an instance of the KafkaWriteStream
   */
  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Properties config) {
    return new KafkaWriteStreamImpl<>(
      vertx,
      new org.apache.kafka.clients.producer.KafkaProducer<>(config),
      KafkaClientOptions.fromProperties(config, true));
  }

  /**
   * Create a new KafkaWriteStream instance
   *
   * @param vertx    Vert.x instance to use
   * @param producer native Kafka producer instance
   */
  static <K, V> KafkaWriteStream<K, V> create(Vertx vertx, Producer<K, V> producer) {
    return new KafkaWriteStreamImpl<>(vertx, producer, new KafkaClientOptions());
  }

  KafkaWriteStream<K, V> exceptionHandler(Handler<Throwable> handler);

  /**
   * Asynchronously write a record to a topic
   *
   * @param record record to write
   * @return a {@code Future} completed with the record metadata
   */
  Future<RecordMetadata> send(ProducerRecord<K, V> record);

  /**
   * Invoking this method makes all buffered records immediately available to write
   */
  Future<Void> flush();

  /**
   * Close the stream
   */
  Future<Void> close();

  /**
   * @return the underlying producer
   */
  Producer<K, V> unwrap();
}
