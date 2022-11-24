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
import dev.knative.eventing.kafka.broker.vertx.kafka.common.ProducerTracer;
import dev.knative.eventing.kafka.broker.vertx.kafka.producer.KafkaWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.List;

/**
 * Kafka write stream implementation
 */
public class KafkaWriteStreamImpl<K, V> implements KafkaWriteStream<K, V> {

  private long maxSize = DEFAULT_MAX_SIZE;
  private long pending;
  private final Producer<K, V> producer;
  private Handler<Void> drainHandler;
  private Handler<Throwable> exceptionHandler;
  private final VertxInternal vertx;
  private final ProducerTracer tracer;
  private final TaskQueue taskQueue;

  public KafkaWriteStreamImpl(Vertx vertx, Producer<K, V> producer, KafkaClientOptions options) {
    ContextInternal ctxInt = ((ContextInternal) vertx.getOrCreateContext()).unwrap();
    this.producer = producer;
    this.vertx = (VertxInternal) vertx;
    this.tracer = ProducerTracer.create(ctxInt.tracer(), options);
    this.taskQueue = new TaskQueue();
  }

  private int len(Object value) {
    if (value instanceof byte[]) {
      return ((byte[])value).length;
    } else if (value instanceof String) {
      return ((String)value).length();
    } else {
      return 1;
    }
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    ContextInternal ctx = vertx.getOrCreateContext();
    ProducerTracer.StartedSpan startedSpan = this.tracer == null ? null : this.tracer.prepareSendMessage(ctx, record);
    int len = this.len(record.value());
    this.pending += len;
    return ctx.executeBlocking(prom -> {
      try {
        this.producer.send(record, (metadata, err) -> {

          // callback from Kafka IO thread
          ctx.runOnContext(v1 -> {
            synchronized (KafkaWriteStreamImpl.this) {

              // if exception happens, no record written
              if (err != null) {

                if (this.exceptionHandler != null) {
                  Handler<Throwable> exceptionHandler = this.exceptionHandler;
                  ctx.runOnContext(v2 -> exceptionHandler.handle(err));
                }
              }

              long lowWaterMark = this.maxSize / 2;
              this.pending -= len;
              if (this.pending < lowWaterMark && this.drainHandler != null) {
                Handler<Void> drainHandler = this.drainHandler;
                this.drainHandler = null;
                ctx.runOnContext(drainHandler);
              }
            }
          });

          if (err != null) {
            if (startedSpan != null) {
              startedSpan.fail(ctx, err);
            }
            prom.fail(err);
          } else {
            if (startedSpan != null) {
              startedSpan.finish(ctx);
            }
            prom.complete(metadata);
          }
        });
      } catch (Throwable e) {
        synchronized (KafkaWriteStreamImpl.this) {
          if (this.exceptionHandler != null) {
            Handler<Throwable> exceptionHandler = this.exceptionHandler;
            ctx.runOnContext(v3 -> exceptionHandler.handle(e));
          }
        }
        if (startedSpan != null) {
          startedSpan.fail(ctx, e);
        }
        prom.fail(e);
      }
    }, taskQueue);
  }

  @Override
  public synchronized KafkaWriteStreamImpl<K, V> send(ProducerRecord<K, V> record, Handler<AsyncResult<RecordMetadata>> handler) {
    this.send(record).onComplete(handler);
    return this;
  }

  @Override
  public void write(ProducerRecord<K, V> data, Handler<AsyncResult<Void>> handler) {
    this.write(data).onComplete(handler);
  }

  @Override
  public Future<Void> write(ProducerRecord<K, V> record) {
    return this.send(record).mapEmpty();
  }

  @Override
  public KafkaWriteStreamImpl<K, V> setWriteQueueMaxSize(int size) {
    this.maxSize = size;
    return this;
  }

  @Override
  public synchronized boolean writeQueueFull() {
    return (this.pending >= this.maxSize);
  }

  @Override
  public synchronized KafkaWriteStreamImpl<K, V> drainHandler(Handler<Void> handler) {
    this.drainHandler = handler;
    return this;
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    if (handler != null) {
      vertx.runOnContext(v -> handler.handle(Future.succeededFuture()));
    }
  }

  @Override
  public KafkaWriteStream<K, V> initTransactions(Handler<AsyncResult<Void>> handler) {
    initTransactions().onComplete(handler);
    return this;
  }

  @Override
  public Future<Void> initTransactions() {
    return executeBlocking(this.producer::initTransactions);
  }

  @Override
  public KafkaWriteStream<K, V> beginTransaction(Handler<AsyncResult<Void>> handler) {
    beginTransaction().onComplete(handler);
    return this;
  }

  @Override
  public Future<Void> beginTransaction() {
    return executeBlocking(this.producer::beginTransaction);
  }

  @Override
  public KafkaWriteStream<K, V> commitTransaction(Handler<AsyncResult<Void>> handler) {
    commitTransaction().onComplete(handler);
    return this;
  }

  @Override
  public Future<Void> commitTransaction() {
    return executeBlocking(this.producer::commitTransaction);
  }

  @Override
  public KafkaWriteStream<K, V> abortTransaction(Handler<AsyncResult<Void>> handler) {
    abortTransaction().onComplete(handler);
    return this;
  }

  @Override
  public Future<Void> abortTransaction() {
    return executeBlocking(this.producer::abortTransaction);
  }

  @Override
  public KafkaWriteStreamImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<List<PartitionInfo>> partitionsFor(String topic) {
    ContextInternal ctx = vertx.getOrCreateContext();
    Promise<List<PartitionInfo>> trampolineProm = ctx.promise();

    // TODO: should be this timeout related to the Kafka producer property "metadata.fetch.timeout.ms" ?
    this.vertx.setTimer(2000, id -> {
      trampolineProm.tryFail("Kafka connect timeout");
    });

    ctx.<List<PartitionInfo>>executeBlocking(prom -> {
      prom.complete(
        this.producer.partitionsFor(topic)
      );
    }, taskQueue).onComplete(trampolineProm);

    return trampolineProm.future(); // Trampoline on caller context
  }

  @Override
  public KafkaWriteStreamImpl<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {
    partitionsFor(topic).onComplete(handler);
    return this;
  }

  @Override
  public Future<Void> flush() {
    ContextInternal ctx = vertx.getOrCreateContext();
    return ctx.<Void>executeBlocking(prom -> {
      this.producer.flush();
      prom.complete();
    }, taskQueue);
  }

  @Override
  public KafkaWriteStreamImpl<K, V> flush(Handler<AsyncResult<Void>> completionHandler) {
    flush().onComplete(completionHandler);
    return this;
  }

  @Override
  public Future<Void> close() {
    return close(0);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    close(0, completionHandler);
  }

  @Override
  public Future<Void> close(long timeout) {
    ContextInternal ctx = vertx.getOrCreateContext();
    Promise<Void> trampolineProm = ctx.promise();
    return ctx.executeBlocking(prom -> {
      if (timeout > 0) {
        this.producer.close(Duration.ofMillis(timeout));
      } else {
        this.producer.close();
      }
      prom.complete();
    }, taskQueue);
  }

  @Override
  public void close(long timeout, Handler<AsyncResult<Void>> completionHandler) {
    close(timeout).onComplete(completionHandler);
  }

  @Override
  public Producer<K, V> unwrap() {
    return this.producer;
  }

  Future<Void> executeBlocking(final BlockingStatement statement) {
    ContextInternal ctx = vertx.getOrCreateContext();
    return ctx.executeBlocking(promise -> {
      try {
        statement.execute();
        promise.complete();
      } catch (Exception e) {
        promise.fail(e);
      }
    }, taskQueue);
  }

  @FunctionalInterface
  private interface BlockingStatement {

    void execute();
  }
}
