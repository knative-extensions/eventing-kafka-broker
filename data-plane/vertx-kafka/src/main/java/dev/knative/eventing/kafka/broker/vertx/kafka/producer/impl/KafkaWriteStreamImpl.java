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
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Kafka write stream implementation
 */
public class KafkaWriteStreamImpl<K, V> implements KafkaWriteStream<K, V> {

  private final Producer<K, V> producer;
  private Handler<Throwable> exceptionHandler;
  private final VertxInternal vertx;
  private final ProducerTracer<?> tracer;
  private final TaskQueue taskQueue;

  public KafkaWriteStreamImpl(Vertx vertx, Producer<K, V> producer, KafkaClientOptions options) {
    ContextInternal ctxInt = ((ContextInternal) vertx.getOrCreateContext()).unwrap();
    this.producer = producer;
    this.vertx = (VertxInternal) vertx;
    this.tracer = ProducerTracer.create(ctxInt.tracer(), options);
    this.taskQueue = new TaskQueue();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    ContextInternal ctx = vertx.getOrCreateContext();
    final var startedSpan = this.tracer == null ? null : this.tracer.prepareSendMessage(ctx, record);
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
  public KafkaWriteStreamImpl<K, V> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<Void> flush() {
    return executeBlocking(producer::flush);
  }

  @Override
  public Future<Void> close() {
    return executeBlocking(producer::close);
  }

  @Override
  public Producer<K, V> unwrap() {
    return this.producer;
  }

  private Future<Void> executeBlocking(final BlockingStatement statement) {
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
