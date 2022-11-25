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

import dev.knative.eventing.kafka.broker.vertx.kafka.common.impl.Helper;
import dev.knative.eventing.kafka.broker.vertx.kafka.consumer.KafkaReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Kafka read stream implementation
 */
public class KafkaReadStreamImpl<K, V> implements KafkaReadStream<K, V> {

  private static final AtomicInteger threadCount = new AtomicInteger(0);

  private final Context context;
  private final AtomicBoolean closed = new AtomicBoolean(true);
  private final Consumer<K, V> consumer;

  private Handler<Throwable> exceptionHandler;
  private Handler<Set<TopicPartition>> partitionsRevokedHandler;
  private Handler<Set<TopicPartition>> partitionsAssignedHandler;

  private ExecutorService worker;

  private final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

      Handler<Set<TopicPartition>> handler = partitionsRevokedHandler;
      if (handler != null) {
        context.runOnContext(v -> handler.handle(Helper.toSet(partitions)));
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

      Handler<Set<TopicPartition>> handler = partitionsAssignedHandler;
      if (handler != null) {
        context.runOnContext(v -> handler.handle(Helper.toSet(partitions)));
      }
    }
  };

  public KafkaReadStreamImpl(Vertx vertx, Consumer<K, V> consumer) {
    ContextInternal ctxInt = ((ContextInternal) vertx.getOrCreateContext()).unwrap();
    this.consumer = consumer;
    this.context = ctxInt;
  }

  private <T> void start(BiConsumer<Consumer<K, V>, Promise<T>> task, Handler<AsyncResult<T>> handler) {
    this.worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "vert.x-kafka-consumer-thread-" + threadCount.getAndIncrement()));
    this.submitTaskWhenStarted(task, handler);
  }

  private <T> void submitTaskWhenStarted(BiConsumer<Consumer<K, V>, Promise<T>> task, Handler<AsyncResult<T>> handler) {
    if (worker == null) {
      throw new IllegalStateException();
    }
    this.worker.submit(() -> {
      Promise<T> future = null;
      if (handler != null) {
        future = Promise.promise();
        future.future().onComplete(event -> {
          // When we've executed the task on the worker thread,
          // run the callback on the eventloop thread
          this.context.runOnContext(v -> handler.handle(event));
        });
      }
      try {
        task.accept(this.consumer, future);
      } catch (Exception e) {
        if (future != null) {
          future.tryFail(e);
        }
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        }
      }
    });
  }

  protected <T> void submitTask(BiConsumer<Consumer<K, V>, Promise<T>> task,
                                Handler<AsyncResult<T>> handler) {
    if (this.closed.compareAndSet(true, false)) {
      this.start(task, handler);
    } else {
      this.submitTaskWhenStarted(task, handler);
    }
  }

  @Override
  public Future<Void> pause(Set<TopicPartition> topicPartitions) {
    Promise<Void> promise = Promise.promise();
    pause(topicPartitions, promise);
    return promise.future();
  }

  private void pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.submitTask((consumer, future) -> {
      consumer.pause(topicPartitions);
      if (future != null) {
        future.complete();
      }
    }, completionHandler);
  }

  @Override
  public Future<Void> resume(Set<TopicPartition> topicPartitions) {
    Promise<Void> promise = Promise.promise();
    this.resume(topicPartitions, promise);
    return promise.future();
  }

  private void resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.submitTask((consumer, future) -> {
      consumer.resume(topicPartitions);
      if (future != null) {
        future.complete();
      }
    }, completionHandler);
  }

  @Override
  public KafkaReadStream<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler) {
    this.partitionsRevokedHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStream<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler) {
    this.partitionsAssignedHandler = handler;
    return this;
  }

  @Override
  public Future<Void> subscribe(Set<String> topics) {
    Promise<Void> promise = Promise.promise();
    subscribe(topics, promise);
    return promise.future();
  }

  private void subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler) {

    BiConsumer<Consumer<K, V>, Promise<Void>> handler = (consumer, future) -> {
      consumer.subscribe(topics, this.rebalanceListener);
      if (future != null) {
        future.complete();
      }
    };

    if (this.closed.compareAndSet(true, false)) {
      this.start(handler, completionHandler);
    } else {
      this.submitTask(handler, completionHandler);
    }
  }

  @Override
  public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();
    this.commit(offsets, promise);
    return promise.future();
  }

  private void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
    this.submitTask((consumer, future) -> {

      if (offsets == null) {
        consumer.commitSync();
      } else {
        consumer.commitSync(offsets);
      }
      if (future != null) {
        future.complete(offsets);
      }

    }, completionHandler);
  }

  @Override
  public KafkaReadStreamImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<Void> close() {
    final ContextInternal ctx = (ContextInternal) this.context;
    if (this.closed.compareAndSet(false, true)) {
      // Call wakeup before closing the consumer, so that existing tasks in the executor queue will
      // wake up while we wait for processing the below added "close" task.
      this.consumer.wakeup();

      final Promise<Void> promise = ctx.promise();

      this.worker.submit(() -> {
        try {
          this.consumer.close();
          promise.complete();
        } catch (final KafkaException ex) {
          promise.fail(ex);
        }
      });

      return promise.future().onComplete(v -> this.worker.shutdownNow());
    }
    return ctx.succeededFuture();
  }

  @Override
  public Consumer<K, V> unwrap() {
    return this.consumer;
  }

  public Future<ConsumerRecords<K, V>> poll(final Duration timeout) {
    final Promise<ConsumerRecords<K, V>> promise = Promise.promise();
    poll(timeout, promise);
    return promise.future();
  }

  private void poll(final Duration timeout, final Handler<AsyncResult<ConsumerRecords<K, V>>> handler) {
    this.worker.submit(() -> {
      if (!this.closed.get()) {
        try {
          ConsumerRecords<K, V> records = this.consumer.poll(timeout);
          this.context.runOnContext(v -> handler.handle(Future.succeededFuture(records)));
        } catch (WakeupException ignore) {
          this.context.runOnContext(v -> handler.handle(Future.succeededFuture(ConsumerRecords.empty())));
        } catch (Exception e) {
          this.context.runOnContext(v -> handler.handle(Future.failedFuture(e)));
        }
      }
    });
  }
}
