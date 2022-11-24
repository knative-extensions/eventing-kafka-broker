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
import dev.knative.eventing.kafka.broker.vertx.kafka.common.PartitionInfo;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.impl.KafkaProducerImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Vert.x Kafka producer.
 * <p>
 * The {@link #write(Object)} provides global control over writing a record.
 */
public interface KafkaProducer<K, V> extends WriteStream<KafkaProducerRecord<K, V>> {

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx  Vert.x instance to use
   * @param name   the producer name to identify it
   * @param config Kafka producer configuration
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config) {
    return KafkaProducerImpl.createShared(vertx, name, config);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx  Vert.x instance to use
   * @param name   the producer name to identify it
   * @param config Kafka producer configuration
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config) {
    return KafkaProducerImpl.createShared(vertx, name, config);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx   Vert.x instance to use
   * @param name    the producer name to identify it
   * @param options Kafka producer options
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options) {
    return KafkaProducerImpl.createShared(vertx, name, options);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx           Vert.x instance to use
   * @param name            the producer name to identify it
   * @param config          Kafka producer configuration
   * @param keySerializer   key serializer
   * @param valueSerializer value serializer
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return KafkaProducerImpl.createShared(vertx, name, config, keySerializer, valueSerializer);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx     Vert.x instance to use
   * @param name      the producer name to identify it
   * @param config    Kafka producer configuration
   * @param keyType   class type for the key serialization
   * @param valueType class type for the value serialization
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    return KafkaProducerImpl.createShared(vertx, name, config, keyType, valueType);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx           Vert.x instance to use
   * @param name            the producer name to identify it
   * @param config          Kafka producer configuration
   * @param keySerializer   key serializer
   * @param valueSerializer value serializer
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return KafkaProducerImpl.createShared(vertx, name, config, keySerializer, valueSerializer);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx     Vert.x instance to use
   * @param name      the producer name to identify it
   * @param config    Kafka producer configuration
   * @param keyType   class type for the key serialization
   * @param valueType class type for the value serialization
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config, Class<K> keyType, Class<V> valueType) {
    return KafkaProducerImpl.createShared(vertx, name, config, keyType, valueType);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx           Vert.x instance to use
   * @param name            the producer name to identify it
   * @param options         Kafka producer options
   * @param keySerializer   key serializer
   * @param valueSerializer value serializer
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return KafkaProducerImpl.createShared(vertx, name, options, keySerializer, valueSerializer);
  }

  /**
   * Get or create a KafkaProducer instance which shares its stream with any other KafkaProducer created with the same {@code name}
   * <p>
   * When {@code close} has been called for each shared producer the resources will be released.
   * Calling {@code end} closes all shared producers.
   *
   * @param vertx     Vert.x instance to use
   * @param name      the producer name to identify it
   * @param options   Kafka producer options
   * @param keyType   class type for the key serialization
   * @param valueType class type for the value serialization
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options, Class<K> keyType, Class<V> valueType) {
    return KafkaProducerImpl.createShared(vertx, name, options, keyType, valueType);
  }

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
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config));
    return new KafkaProducerImpl<>(vertx, stream).registerCloseHook();
  }

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx           Vert.x instance to use
   * @param config          Kafka producer configuration
   * @param keySerializer   key serializer
   * @param valueSerializer value serializer
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config), keySerializer, valueSerializer);
    return new KafkaProducerImpl<>(vertx, stream).registerCloseHook();
  }

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx     Vert.x instance to use
   * @param config    Kafka producer configuration
   * @param keyType   class type for the key serialization
   * @param valueType class type for the value serialization
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, new HashMap<>(config), keyType, valueType);
    return new KafkaProducerImpl<>(vertx, stream).registerCloseHook();
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

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx           Vert.x instance to use
   * @param config          Kafka producer configuration
   * @param keySerializer   key serializer
   * @param valueSerializer value serializer
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config, keySerializer, valueSerializer);
    return new KafkaProducerImpl<>(vertx, stream).registerCloseHook();
  }

  /**
   * Create a new KafkaProducer instance
   *
   * @param vertx     Vert.x instance to use
   * @param config    Kafka producer configuration
   * @param keyType   class type for the key serialization
   * @param valueType class type for the value serialization
   * @return an instance of the KafkaProducer
   */
  static <K, V> KafkaProducer<K, V> create(Vertx vertx, Properties config, Class<K> keyType, Class<V> valueType) {
    KafkaWriteStream<K, V> stream = KafkaWriteStream.create(vertx, config, keyType, valueType);
    return new KafkaProducerImpl<>(vertx, stream).registerCloseHook();
  }

  /**
   * Initializes the underlying kafka transactional producer. See {@link KafkaProducer#initTransactions()} ()}
   *
   * @param handler handler called on operation completed
   * @return current KafkaWriteStream instance
   */
  KafkaProducer<K, V> initTransactions(Handler<AsyncResult<Void>> handler);

  /**
   * Like {@link #initTransactions(Handler)} but with a future of the result
   */
  Future<Void> initTransactions();

  /**
   * Starts a new kafka transaction. See {@link KafkaProducer#beginTransaction()}
   *
   * @param handler handler called on operation completed
   * @return current KafkaWriteStream instance
   */
  KafkaProducer<K, V> beginTransaction(Handler<AsyncResult<Void>> handler);

  /**
   * Like {@link #beginTransaction(Handler)} but with a future of the result
   */
  Future<Void> beginTransaction();

  /**
   * Commits the ongoing transaction. See {@link KafkaProducer#commitTransaction()}
   *
   * @param handler handler called on operation completed
   * @return current KafkaWriteStream instance
   */
  KafkaProducer<K, V> commitTransaction(Handler<AsyncResult<Void>> handler);

  /**
   * Like {@link #commitTransaction(Handler)} but with a future of the result
   */
  Future<Void> commitTransaction();

  /**
   * Aborts the ongoing transaction. See {@link org.apache.kafka.clients.producer.KafkaProducer#abortTransaction()}
   *
   * @param handler handler called on operation completed
   * @return current KafkaWriteStream instance
   */
  KafkaProducer<K, V> abortTransaction(Handler<AsyncResult<Void>> handler);

  /**
   * Like {@link #abortTransaction(Handler)} but with a future of the result
   */
  Future<Void> abortTransaction();

  @Override
  KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler);

  @Override
  KafkaProducer<K, V> setWriteQueueMaxSize(int i);

  @Override
  KafkaProducer<K, V> drainHandler(Handler<Void> handler);

  /**
   * Asynchronously write a record to a topic
   *
   * @param record record to write
   * @return a {@code Future} completed with the record metadata
   */
  Future<RecordMetadata> send(KafkaProducerRecord<K, V> record);

  /**
   * Asynchronously write a record to a topic
   *
   * @param record  record to write
   * @param handler handler called on operation completed
   * @return current KafkaWriteStream instance
   */
  KafkaProducer<K, V> send(KafkaProducerRecord<K, V> record, Handler<AsyncResult<RecordMetadata>> handler);

  /**
   * Get the partition metadata for the give topic.
   *
   * @param topic   topic partition for which getting partitions info
   * @param handler handler called on operation completed
   * @return current KafkaProducer instance
   */
  KafkaProducer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler);

  /**
   * Like {@link #partitionsFor(String, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<List<PartitionInfo>> partitionsFor(String topic);

  /**
   * Invoking this method makes all buffered records immediately available to write
   *
   * @param completionHandler handler called on operation completed
   * @return current KafkaProducer instance
   */
  KafkaProducer<K, V> flush(Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #flush(Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> flush();

  /**
   * Close the producer
   *
   * @return a {@code Future} completed with the operation result
   */
  Future<Void> close();

  /**
   * Close the producer
   *
   * @param completionHandler handler called on operation completed
   */
  void close(Handler<AsyncResult<Void>> completionHandler);

  /**
   * Like {@link #close(long, Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> close(long timeout);

  /**
   * Close the producer
   *
   * @param timeout           timeout to wait for closing
   * @param completionHandler handler called on operation completed
   */
  void close(long timeout, Handler<AsyncResult<Void>> completionHandler);

  /**
   * @return underlying {@link KafkaWriteStream} instance
   */
  KafkaWriteStream<K, V> asStream();

  /**
   * @return the underlying producer
   */
  Producer<K, V> unwrap();
}
