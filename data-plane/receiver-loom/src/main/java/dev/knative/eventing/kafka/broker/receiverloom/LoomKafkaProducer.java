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
package dev.knative.eventing.kafka.broker.receiverloom;

import com.google.common.util.concurrent.Uninterruptibles;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.tracing.kafka.ProducerTracer;
import io.opentelemetry.context.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.tracing.TracingPolicy;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoomKafkaProducer<K, V> implements ReactiveKafkaProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(LoomKafkaProducer.class);

    private final Producer<K, V> producer;

    private final ExecutorService executorService;
    private final AtomicBoolean isClosed;
    private final ProducerTracer<?> tracer;
    private final VertxInternal vertx;
    private final Promise<Void> closePromise = Promise.promise();

    public LoomKafkaProducer(Vertx v, Producer<K, V> producer) {
        Objects.requireNonNull(v, "Vertx cannot be null");
        this.producer = producer;
        this.isClosed = new AtomicBoolean(false);
        this.vertx = (VertxInternal) v;
        final var ctxInt = ((ContextInternal) v.getOrCreateContext()).unwrap();
        if (ctxInt.tracer() != null) {
            this.tracer =
                    new ProducerTracer(ctxInt.tracer(), TracingPolicy.PROPAGATE, "" /* TODO add bootrstrap servers */);
        } else {
            this.tracer = null;
        }

        ExecutorService executorService;
        if (Boolean.parseBoolean(System.getenv("ENABLE_VIRTUAL_THREADS"))) {
            executorService = Executors.newVirtualThreadPerTaskExecutor();
        } else {
            executorService = Executors.newSingleThreadExecutor();
        }
        this.executorService = Context.taskWrapping(executorService);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        if (isClosed.get()) {
            return Future.failedFuture("Producer is closed");
        }
        PromiseInternal<RecordMetadata> promise = vertx.promise();
        executorService.execute(() -> sendFromQueue(new RecordPromise<>(record, promise)));
        return promise.future();
    }

    private void sendFromQueue(RecordPromise<K, V> recordPromise) {
        final var startedSpan = this.tracer == null
                ? null
                : this.tracer.prepareSendMessage(recordPromise.context(), recordPromise.record);

        recordPromise
                .promise
                .future()
                .onComplete(v -> {
                    if (startedSpan != null) {
                        startedSpan.finish(recordPromise.context());
                    }
                })
                .onFailure(cause -> {
                    if (startedSpan != null) {
                        startedSpan.fail(recordPromise.context(), cause);
                    }
                });
        try {
            producer.send(recordPromise.record, (metadata, exception) -> {
                if (exception != null) {
                    recordPromise.fail(exception);
                    return;
                }
                recordPromise.complete(metadata);
            });
        } catch (final KafkaException exception) {
            recordPromise.fail(exception);
        }
    }

    @Override
    public Future<Void> close() {
        if (!this.isClosed.compareAndSet(false, true)) {
            return closePromise.future();
        }

        logger.debug("Closing producer");

        Thread.ofVirtual().start(() -> {
            try {
                executorService.shutdown();
                logger.debug("Waiting for tasks to complete");
                Uninterruptibles.awaitTerminationUninterruptibly(executorService);
                logger.debug("Closing the producer");
                producer.close();
                closePromise.complete();
            } catch (Exception e) {
                closePromise.fail(e);
            }
        });

        return closePromise.future();
    }

    @Override
    public Future<Void> flush() {
        final Promise<Void> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
                producer.flush();
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        });
        return promise.future();
    }

    @Override
    public Producer<K, V> unwrap() {
        return producer;
    }

    private static class RecordPromise<K, V> {
        final ProducerRecord<K, V> record;
        final PromiseInternal<RecordMetadata> promise;

        RecordPromise(ProducerRecord<K, V> record, PromiseInternal<RecordMetadata> promise) {
            this.record = record;
            this.promise = promise;
        }

        ContextInternal context() {
            return promise.context();
        }

        void complete(RecordMetadata result) {
            promise.complete(result);
        }

        void fail(Throwable cause) {
            promise.fail(cause);
        }
    }

    // Function needed for testing
    public boolean isSendFromQueueThreadAlive() {
        return !executorService.isTerminated();
    }
}
