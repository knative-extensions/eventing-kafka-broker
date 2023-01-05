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

import dev.knative.eventing.kafka.broker.vertx.kafka.common.ConsumerTracer;
import dev.knative.eventing.kafka.broker.vertx.kafka.common.KafkaClientOptions;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

/**
 * Kafka read stream implementation
 */
public class KafkaReadStreamImpl<K, V> implements KafkaReadStream<K, V> {

  private static final AtomicInteger threadCount = new AtomicInteger(0);

  private final Context context;
  private final AtomicBoolean closed = new AtomicBoolean(true);
  private final Consumer<K, V> consumer;
  private final ConsumerTracer tracer;

  private final AtomicBoolean consuming = new AtomicBoolean(false);
  private final AtomicLong demand = new AtomicLong(Long.MAX_VALUE);
  private final AtomicBoolean polling = new AtomicBoolean(false);
  private Handler<ConsumerRecord<K, V>> recordHandler;
  private Handler<Throwable> exceptionHandler;
  private Iterator<ConsumerRecord<K, V>> current; // Accessed on event loop
  private Handler<ConsumerRecords<K, V>> batchHandler;
  private Handler<Set<TopicPartition>> partitionsRevokedHandler;
  private Handler<Set<TopicPartition>> partitionsAssignedHandler;
  private Duration pollTimeout = Duration.ofSeconds(1);

  private ExecutorService worker;

  private final ConsumerRebalanceListener rebalanceListener =  new ConsumerRebalanceListener() {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

      Handler<Set<TopicPartition>> handler = partitionsRevokedHandler;
      if (handler != null) {
        context.runOnContext(v -> {
          handler.handle(Helper.toSet(partitions));
        });
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

      Handler<Set<TopicPartition>> handler = partitionsAssignedHandler;
      if (handler != null) {
        context.runOnContext(v -> {
          handler.handle(Helper.toSet(partitions));
        });
      }
    }
  };

  public KafkaReadStreamImpl(Vertx vertx, Consumer<K, V> consumer, KafkaClientOptions options) {
    ContextInternal ctxInt = ((ContextInternal) vertx.getOrCreateContext()).unwrap();
    this.consumer = consumer;
    this.context = ctxInt;
    this.tracer = ConsumerTracer.create(ctxInt.tracer(), options);
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
        future.future().onComplete(event-> {
          // When we've executed the task on the worker thread,
          // run the callback on the eventloop thread
          this.context.runOnContext(v-> {
            handler.handle(event);
            });
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

  private void pollRecords(Handler<ConsumerRecords<K, V>> handler) {
      if(this.polling.compareAndSet(false, true)){
          this.worker.submit(() -> {
             boolean submitted = false;
             try {
                if (!this.closed.get()) {
                  try {
                    ConsumerRecords<K, V> records = this.consumer.poll(pollTimeout);
                    if (records != null && records.count() > 0) {
                      submitted = true; // sets false only when the iterator is overwritten
                      this.context.runOnContext(v -> {
                          this.polling.set(false);
                          handler.handle(records);
                      });
                    }
                  } catch (WakeupException ignore) {
                  } catch (Exception e) {
                    if (exceptionHandler != null) {
                      exceptionHandler.handle(e);
                    }
                  }
                }
             } finally {
                 if(!submitted){
                     this.context.runOnContext(v -> {
                         this.polling.set(false);
                         schedule(0);
                     });
                 }
             }
          });
      }
  }

  private void schedule(long delay) {
    Handler<ConsumerRecord<K, V>> handler = this.recordHandler;

    if (this.consuming.get()
        && this.demand.get() > 0L
        && handler != null) {

      this.context.runOnContext(v1 -> {
        if (delay > 0) {
          this.context.owner().setTimer(delay, v2 -> run(handler));
        } else {
          run(handler);
        }
      });
    }
  }

  // Access the consumer from the event loop since the consumer is not thread safe
  private void run(Handler<ConsumerRecord<K, V>> handler) {

    if (this.closed.get()) {
      return;
    }

    if (this.current == null || !this.current.hasNext()) {

      this.pollRecords(records -> {

        if (records != null && records.count() > 0) {
          this.current = records.iterator();
          if (batchHandler != null) {
            batchHandler.handle(records);
          }
          this.schedule(0);
        } else {
          this.schedule(1);
        }
      });

    } else {

      int count = 0;
      out:
      while (this.current.hasNext() && count++ < 10) {

        // to honor the Vert.x ReadStream contract, handler should not be called if stream is paused
        while (true) {
          long v = this.demand.get();
          if (v <= 0L) {
            break out;
          } else if (v == Long.MAX_VALUE || this.demand.compareAndSet(v, v - 1)) {
            break;
          }
        }

        ConsumerRecord<K, V> next = this.current.next();
        ContextInternal ctx = ((ContextInternal)this.context).duplicate();
        ctx.emit(v -> this.tracedHandler(ctx, handler).handle(next));
      }
      this.schedule(0);
    }
  }

  private Handler<ConsumerRecord<K, V>> tracedHandler(Context ctx, Handler<ConsumerRecord<K, V>> handler) {
    return this.tracer == null ? handler :
      rec -> {
        ConsumerTracer.StartedSpan startedSpan = tracer.prepareMessageReceived(ctx, rec);
        try {
          handler.handle(rec);
          startedSpan.finish(ctx);
        } catch (Throwable t) {
          startedSpan.fail(ctx, t);
          throw t;
        }
      };
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

  @Override
  public KafkaReadStream<K, V> pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {

    this.submitTask((consumer, future) -> {
      consumer.pause(topicPartitions);
      if (future != null) {
        future.complete();
      }
    }, completionHandler);

    return this;
  }

  @Override
  public void paused(Handler<AsyncResult<Set<TopicPartition>>> handler) {

    this.submitTask((consumer, future) -> {
      Set<TopicPartition> result = consumer.paused();
      if (future != null) {
        future.complete(result);
      }
    }, handler);
  }

  @Override
  public Future<Set<TopicPartition>> paused() {
    Promise<Set<TopicPartition>> promise = Promise.promise();
    paused(promise);
    return promise.future();
  }

  @Override
  public Future<Void> resume(Set<TopicPartition> topicPartitions) {
    Promise<Void> promise = Promise.promise();
    this.resume(topicPartitions, promise);
    return promise.future();
  }

  @Override
  public KafkaReadStream<K, V> resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {

    this.submitTask((consumer, future) -> {
      consumer.resume(topicPartitions);
      if (future != null) {
        future.complete();
      }
    }, completionHandler);

    return this;
  }

  @Override
  public void committed(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler) {

    this.submitTask((consumer, future) -> {
      OffsetAndMetadata result = consumer.committed(topicPartition);
      if (future != null) {
        future.complete(result);
      }
    }, handler);
  }

  @Override
  public Future<OffsetAndMetadata> committed(TopicPartition topicPartition) {
    Promise<OffsetAndMetadata> promise = Promise.promise();
    committed(topicPartition, promise);
    return promise.future();
  }

  @Override
  public Future<Void> seekToEnd(Set<TopicPartition> topicPartitions) {
    Promise<Void> promise = Promise.promise();
    this.seekToEnd(topicPartitions, promise);
    return promise.future();
  }

  @Override
  public KafkaReadStream<K, V> seekToEnd(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.context.runOnContext(r -> {
      current = null;

      this.submitTask((consumer, future) -> {
        consumer.seekToEnd(topicPartitions);
        if (future != null) {
          future.complete();
        }
      }, completionHandler);
    });
    return this;
  }

  @Override
  public Future<Void> seekToBeginning(Set<TopicPartition> topicPartitions) {
    Promise<Void> promise = Promise.promise();
    this.seekToBeginning(topicPartitions, promise);
    return promise.future();
  }

  @Override
  public KafkaReadStream<K, V> seekToBeginning(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
    this.context.runOnContext(r -> {
      current = null;

      this.submitTask((consumer, future) -> {
        consumer.seekToBeginning(topicPartitions);
        if (future != null) {
          future.complete();
        }
      }, completionHandler);
    });
    return this;
  }

  @Override
  public Future<Void> seek(TopicPartition topicPartition, long offset) {
    Promise<Void> promise = Promise.promise();
    this.seek(topicPartition, offset, promise);
    return promise.future();
  }

  @Override
  public KafkaReadStream<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler) {
    this.context.runOnContext(r -> {
      current = null;

      this.submitTask((consumer, future) -> {
        consumer.seek(topicPartition, offset);
        if (future != null) {
          future.complete();
        }
      }, completionHandler);
    });

    return this;
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

  @Override
  public KafkaReadStream<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler) {

    BiConsumer<Consumer<K, V>, Promise<Void>> handler = (consumer, future) -> {
      consumer.subscribe(topics, this.rebalanceListener);
      this.startConsuming();
      if (future != null) {
        future.complete();
      }
    };

    if (this.closed.compareAndSet(true, false)) {
      this.start(handler, completionHandler);
    } else {
      this.submitTask(handler, completionHandler);
    }

    return this;
  }

  @Override
  public KafkaReadStream<K, V> subscribe(Pattern pattern, Handler<AsyncResult<Void>> completionHandler) {

    BiConsumer<Consumer<K, V>, Promise<Void>> handler = (consumer, future) -> {
      consumer.subscribe(pattern, this.rebalanceListener);
      this.startConsuming();
      if (future != null) {
        future.complete();
      }
    };

    if (this.closed.compareAndSet(true, false)) {
      this.start(handler, completionHandler);
    } else {
      this.submitTask(handler, completionHandler);
    }

    return this;
  }

  @Override
  public Future<Void> subscribe(Pattern pattern) {
    Promise<Void> promise = Promise.promise();
    subscribe(pattern, promise);
    return promise.future();
  }

  @Override
  public Future<Void> unsubscribe() {
    Promise<Void> promise = Promise.promise();
    this.unsubscribe(promise);
    return promise.future();
  }

  @Override
  public KafkaReadStream<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler) {

    this.submitTask((consumer, future) -> {
      consumer.unsubscribe();
      if (future != null) {
        future.complete();
      }
    }, completionHandler);

    return this;
  }

  @Override
  public KafkaReadStream<K, V> subscription(Handler<AsyncResult<Set<String>>> handler) {

    this.submitTask((consumer, future) -> {
      Set<String> subscription = consumer.subscription();
      if (future != null) {
        future.complete(subscription);
      }
    }, handler);

    return this;
  }

  @Override
  public Future<Set<String>> subscription() {
    Promise<Set<String>> promise = Promise.promise();
    subscription(promise);
    return promise.future();
  }

  @Override
  public Future<Void> assign(Set<TopicPartition> partitions) {
    Promise<Void> promise = Promise.promise();
    this.assign(partitions, promise);
    return promise.future();
  }

  @Override
  public KafkaReadStream<K, V> assign(Set<TopicPartition> partitions, Handler<AsyncResult<Void>> completionHandler) {

    BiConsumer<Consumer<K, V>, Promise<Void>> handler = (consumer, future) -> {
      consumer.assign(partitions);
      this.startConsuming();
      if (future != null) {
        future.complete();
      }
    };

    if (this.closed.compareAndSet(true, false)) {
      this.start(handler, completionHandler);
    } else {
      this.submitTask(handler, completionHandler);
    }

    return this;
  }

  @Override
  public KafkaReadStream<K, V> assignment(Handler<AsyncResult<Set<TopicPartition>>> handler) {

    this.submitTask((consumer, future) -> {
      Set<TopicPartition> partitions = consumer.assignment();
      if (future != null) {
        future.complete(partitions);
      }
    }, handler);

    return this;
  }

  @Override
  public Future<Set<TopicPartition>> assignment() {
    Promise<Set<TopicPartition>> promise = Promise.promise();
    assignment(promise);
    return promise.future();
  }

  @Override
  public KafkaReadStream<K, V> listTopics(Handler<AsyncResult<Map<String,List<PartitionInfo>>>> handler) {

    this.submitTask((consumer, future) -> {
      Map<String, List<PartitionInfo>> topics = consumer.listTopics();
      if (future != null) {
        future.complete(topics);
      }
    }, handler);

    return this;
  }

  @Override
  public Future<Map<String, List<PartitionInfo>>> listTopics() {
    Promise<Map<String, List<PartitionInfo>>> promise = Promise.promise();
    listTopics(promise);
    return promise.future();
  }

  @Override
  public Future<Map<TopicPartition, OffsetAndMetadata>> commit() {
    Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();
    this.commit(promise);
    return promise.future();
  }

  @Override
  public void commit(Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
    this.commit(null, completionHandler);
  }

  @Override
  public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();
    this.commit(offsets, promise);
    return promise.future();
  }

  @Override
  public void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
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
  public KafkaReadStreamImpl<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {

    this.submitTask((consumer, future) -> {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      if (future != null) {
        future.complete(partitions);
      }
    }, handler);

    return this;
  }

  @Override
  public Future<List<PartitionInfo>> partitionsFor(String topic) {
    Promise<List<PartitionInfo>> promise = Promise.promise();
    partitionsFor(topic, promise);
    return promise.future();
  }

  @Override
  public KafkaReadStreamImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> handler(Handler<ConsumerRecord<K, V>> handler) {
    this.recordHandler = handler;
    this.schedule(0);
    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> pause() {
    this.demand.set(0L);
    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public KafkaReadStreamImpl<K, V> fetch(long amount) {
    if (amount < 0) {
      throw new IllegalArgumentException("Invalid claim " + amount);
    }
    ;
    long op = this.demand.updateAndGet(val -> {
      val += amount;
      if (val < 0L) {
        val = Long.MAX_VALUE;
      }
      return val;
    });
    if (op > 0L) {
      this.schedule(0);
    }
    return this;
  }

  @Override
  public long demand() {
    return this.demand.get();
  }

  private KafkaReadStreamImpl<K, V> startConsuming() {
    this.consuming.set(true);
    this.schedule(0);
    return this;
  }

  @Override
  public KafkaReadStreamImpl<K, V> endHandler(Handler<Void> endHandler) {
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
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    final Future<Void> f = close();
    if (completionHandler != null) {
      f.onComplete(completionHandler);
    }
  }

  @Override
  public void position(TopicPartition partition, Handler<AsyncResult<Long>> handler) {
    this.submitTask((consumer, future) -> {
      long pos = this.consumer.position(partition);
      if (future != null) {
        future.complete(pos);
      }
    }, handler);
  }

  @Override
  public Future<Long> position(TopicPartition partition) {
    Promise<Long> promise = Promise.promise();
    position(partition, promise);
    return promise.future();
  }

  @Override
  public void offsetsForTimes(Map<TopicPartition, Long> topicPartitionTimestamps, Handler<AsyncResult<Map<TopicPartition, OffsetAndTimestamp>>> handler) {
    this.submitTask((consumer, future) -> {
      Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = this.consumer.offsetsForTimes(topicPartitionTimestamps);
      if (future != null) {
        future.complete(offsetsForTimes);
      }
    }, handler);
  }

  @Override
  public Future<Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(Map<TopicPartition, Long> topicPartitionTimestamps) {
    Promise<Map<TopicPartition, OffsetAndTimestamp>> promise = Promise.promise();
    offsetsForTimes(topicPartitionTimestamps, promise);
    return promise.future();
  }

  @Override
  public void offsetsForTimes(TopicPartition topicPartition, long timestamp, Handler<AsyncResult<OffsetAndTimestamp>> handler) {
    this.submitTask((consumer, future) -> {
      Map<TopicPartition, Long> input = new HashMap<>();
      input.put(topicPartition, timestamp);

      Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = this.consumer.offsetsForTimes(input);
      if (future != null) {
        future.complete(offsetsForTimes.get(topicPartition));
      }
    }, handler);
  }

  @Override
  public Future<OffsetAndTimestamp> offsetsForTimes(TopicPartition topicPartition, long timestamp) {
    Promise<OffsetAndTimestamp> promise = Promise.promise();
    offsetsForTimes(topicPartition, timestamp, promise);
    return promise.future();
  }

  @Override
  public void beginningOffsets(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {
    this.submitTask((consumer, future) -> {
      Map<TopicPartition, Long> beginningOffsets = this.consumer.beginningOffsets(topicPartitions);
      if (future != null) {
        future.complete(beginningOffsets);
      }
    }, handler);
  }

  @Override
  public Future<Map<TopicPartition, Long>> beginningOffsets(Set<TopicPartition> topicPartitions) {
    Promise<Map<TopicPartition, Long>> promise = Promise.promise();
    beginningOffsets(topicPartitions, promise);
    return promise.future();
  }

  @Override
  public void beginningOffsets(TopicPartition topicPartition, Handler<AsyncResult<Long>> handler) {
    this.submitTask((consumer, future) -> {
      Set<TopicPartition> input = new HashSet<>();
      input.add(topicPartition);
      Map<TopicPartition, Long> beginningOffsets = this.consumer.beginningOffsets(input);
      if (future != null) {
        future.complete(beginningOffsets.get(topicPartition));
      }
    }, handler);
  }

  @Override
  public Future<Long> beginningOffsets(TopicPartition topicPartition) {
    Promise<Long> promise = Promise.promise();
    beginningOffsets(topicPartition, promise);
    return promise.future();
  }

  @Override
  public void endOffsets(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {
    this.submitTask((consumer, future) -> {
      Map<TopicPartition, Long> endOffsets = this.consumer.endOffsets(topicPartitions);
      if (future != null) {
        future.complete(endOffsets);
      }
    }, handler);
  }

  @Override
  public Future<Map<TopicPartition, Long>> endOffsets(Set<TopicPartition> topicPartitions) {
    Promise<Map<TopicPartition, Long>> promise = Promise.promise();
    endOffsets(topicPartitions, promise);
    return promise.future();
  }

  @Override
  public void endOffsets(TopicPartition topicPartition, Handler<AsyncResult<Long>> handler) {
    this.submitTask((consumer, future) -> {
      Set<TopicPartition> input = new HashSet<>();
      input.add(topicPartition);
      Map<TopicPartition, Long> endOffsets = this.consumer.endOffsets(input);
      if (future != null) {
        future.complete(endOffsets.get(topicPartition));
      }
    }, handler);
  }

  @Override
  public Future<Long> endOffsets(TopicPartition topicPartition) {
    Promise<Long> promise = Promise.promise();
    endOffsets(topicPartition, promise);
    return promise.future();
  }

  @Override
  public Consumer<K, V> unwrap() {
    return this.consumer;
  }

  public KafkaReadStream batchHandler(Handler<ConsumerRecords<K, V>> handler) {
    this.batchHandler = handler;
    return this;
  }

  @Override
  public KafkaReadStream<K, V> pollTimeout(final Duration timeout) {
    this.pollTimeout = timeout;
    return this;
  }

  @Override
  public void poll(final Duration timeout, final Handler<AsyncResult<ConsumerRecords<K, V>>> handler) {
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

  @Override
  public Future<ConsumerRecords<K, V>> poll(final Duration timeout) {
    final Promise<ConsumerRecords<K, V>> promise = Promise.promise();
    poll(timeout, promise);
    return promise.future();
  }
}
