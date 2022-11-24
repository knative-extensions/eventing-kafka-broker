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
package dev.knative.eventing.kafka.broker.vertx.kafka.producer.impl;

import dev.knative.eventing.kafka.broker.vertx.kafka.common.KafkaClientOptions;
import dev.knative.eventing.kafka.broker.vertx.kafka.common.PartitionInfo;
import dev.knative.eventing.kafka.broker.vertx.kafka.common.impl.CloseHandler;
import dev.knative.eventing.kafka.broker.vertx.kafka.common.impl.Helper;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.KafkaProducer;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.KafkaProducerRecord;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.KafkaWriteStream;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.RecordMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Vert.x Kafka producer implementation
 */
public class KafkaProducerImpl<K, V> implements KafkaProducer<K, V> {

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, config));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, new HashMap<>(config)));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, options));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config, Class<K> keyType, Class<V> valueType) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, config, keyType, valueType));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Properties config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, config, keySerializer, valueSerializer));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config, Class<K> keyType, Class<V> valueType) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, new HashMap<>(config), keyType, valueType));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Map<String, String> config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, new HashMap<>(config), keySerializer, valueSerializer));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options, Class<K> keyType, Class<V> valueType) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, options, keyType, valueType));
  }

  public static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, KafkaClientOptions options, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return createShared(vertx, name, () -> KafkaWriteStream.create(vertx, options, keySerializer, valueSerializer));
  }

  private static class SharedProducer extends HashMap<Object, KafkaProducer> {

    final Producer producer;
    final CloseHandler closeHandler;

    public SharedProducer(KafkaWriteStream stream) {
      this.producer = stream.unwrap();
      this.closeHandler = new CloseHandler(stream::close);
    }
  }

  private static final Map<String, SharedProducer> sharedProducers = new HashMap<>();

  private static <K, V> KafkaProducer<K, V> createShared(Vertx vertx, String name, Supplier<KafkaWriteStream> streamFactory) {
    synchronized (sharedProducers) {
      SharedProducer sharedProducer = sharedProducers.computeIfAbsent(name, key -> {
        KafkaWriteStream stream = streamFactory.get();
        SharedProducer s = new SharedProducer(stream);
        s.closeHandler.registerCloseHook((VertxInternal) vertx);
        return s;
      });
      Object key = new Object();
      KafkaProducerImpl<K, V> producer = new KafkaProducerImpl<>(vertx, KafkaWriteStream.create(vertx, sharedProducer.producer), new CloseHandler((timeout, ar) -> {
        synchronized (sharedProducers) {
          sharedProducer.remove(key);
          if (sharedProducer.isEmpty()) {
            sharedProducers.remove(name);
            sharedProducer.closeHandler.close(timeout, ar);
            return;
          }
        }
        ar.handle(Future.succeededFuture());
      }));
      sharedProducer.put(key, producer);
      return producer.registerCloseHook();
    }
  }

  private final Vertx vertx;
  private final KafkaWriteStream<K, V> stream;
  private final CloseHandler closeHandler;

  public KafkaProducerImpl(Vertx vertx, KafkaWriteStream<K, V> stream, CloseHandler closeHandler) {
    this.vertx = vertx;
    this.stream = stream;
    this.closeHandler = closeHandler;
  }

  public KafkaProducerImpl(Vertx vertx, KafkaWriteStream<K, V> stream) {
    this(vertx, stream, new CloseHandler(stream::close));
  }

  public KafkaProducerImpl<K, V> registerCloseHook() {
    Context context = Vertx.currentContext();
    if (context == null) {
      return this;
    }
    closeHandler.registerCloseHook((ContextInternal) context);
    return this;
  }

  @Override
  public KafkaProducer<K, V> initTransactions(Handler<AsyncResult<Void>> handler) {
    this.stream.initTransactions(handler);
    return this;
  }

  @Override
  public Future<Void> initTransactions() {
    return this.stream.initTransactions();
  }

  @Override
  public KafkaProducer<K, V> beginTransaction(Handler<AsyncResult<Void>> handler) {
    this.stream.beginTransaction(handler);
    return this;
  }

  @Override
  public Future<Void> beginTransaction() {
    return this.stream.beginTransaction();
  }

  @Override
  public KafkaProducer<K, V> commitTransaction(Handler<AsyncResult<Void>> handler) {
    this.stream.commitTransaction(handler);
    return this;
  }

  @Override
  public Future<Void> commitTransaction() {
    return this.stream.commitTransaction();
  }

  @Override
  public KafkaProducer<K, V> abortTransaction(Handler<AsyncResult<Void>> handler) {
    this.stream.abortTransaction(handler);
    return this;
  }

  @Override
  public Future<Void> abortTransaction() {
    return this.stream.abortTransaction();
  }

  @Override
  public KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.stream.exceptionHandler(handler);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Future<Void> write(KafkaProducerRecord<K, V> kafkaProducerRecord) {
    return this.stream.write(kafkaProducerRecord.record());
  }

  @Override
  public void write(KafkaProducerRecord<K, V> record, Handler<AsyncResult<Void>> handler) {
    this.stream.write(record.record(), handler);
  }

  @Override
  public Future<RecordMetadata> send(KafkaProducerRecord<K, V> record) {
    return this.stream.send(record.record()).map(Helper::from);
  }

  @Override
  @SuppressWarnings("unchecked")
  public KafkaProducer<K, V> send(KafkaProducerRecord<K, V> record, Handler<AsyncResult<RecordMetadata>> handler) {
    this.send(record).onComplete(handler);
    return this;
  }

  @Override
  public Future<List<PartitionInfo>> partitionsFor(String topic) {
    return this.stream.partitionsFor(topic).map(list ->
      list.stream().map(kafkaPartitionInfo ->
        new PartitionInfo()
          .setInSyncReplicas(
            Stream.of(kafkaPartitionInfo.inSyncReplicas()).map(Helper::from).collect(Collectors.toList()))
          .setLeader(Helper.from(kafkaPartitionInfo.leader()))
          .setPartition(kafkaPartitionInfo.partition())
          .setReplicas(
            Stream.of(kafkaPartitionInfo.replicas()).map(Helper::from).collect(Collectors.toList()))
          .setTopic(kafkaPartitionInfo.topic())
      ).collect(Collectors.toList())
    );
  }

  @Override
  public KafkaProducer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {
    partitionsFor(topic).onComplete(handler);
    return this;
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    this.stream.end(handler);
  }

  @Override
  public KafkaProducer<K, V> setWriteQueueMaxSize(int size) {
    this.stream.setWriteQueueMaxSize(size);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return this.stream.writeQueueFull();
  }

  @Override
  public KafkaProducer<K, V> drainHandler(Handler<Void> handler) {
    this.stream.drainHandler(handler);
    return this;
  }

  @Override
  public KafkaProducer<K, V> flush(Handler<AsyncResult<Void>> completionHandler) {
    this.stream.flush(completionHandler);
    return this;
  }

  @Override
  public Future<Void> flush() {
    return this.stream.flush();
  }

  @Override
  public Future<Void> close(long timeout) {
    Promise<Void> promise = Promise.promise();
    closeHandler.close(timeout, promise);
    return promise.future();
  }

  @Override
  public Future<Void> close() {
    Promise<Void> promise = Promise.promise();
    closeHandler.close(promise);
    return promise.future();
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    closeHandler.close(completionHandler);
  }

  @Override
  public void close(long timeout, Handler<AsyncResult<Void>> completionHandler) {
    closeHandler.close(completionHandler);
  }

  @Override
  public KafkaWriteStream<K, V> asStream() {
    return this.stream;
  }

  @Override
  public Producer<K, V> unwrap() {
    return this.stream.unwrap();
  }
}
