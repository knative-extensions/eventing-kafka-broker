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

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.tracing.kafka.ProducerTracer;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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

    private final BlockingQueue<RecordPromise<K, V>> eventQueue;
    private final AtomicBoolean isClosed;
    private final ProducerTracer tracer;
    private final VertxInternal vertx;
    private final ContextInternal ctx;
    private final Thread sendFromQueueThread;

    public LoomKafkaProducer(Vertx v, Producer<K, V> producer) {
        Objects.requireNonNull(v, "Vertx cannot be null");
        this.producer = producer;
        this.eventQueue = new LinkedBlockingQueue<>();
        this.isClosed = new AtomicBoolean(false);
        this.vertx = (VertxInternal) v;
        this.ctx = vertx.getOrCreateContext();
        ContextInternal ctxInt = ((ContextInternal) v.getOrCreateContext()).unwrap();
        this.tracer = ProducerTracer.create(ctxInt.tracer());

        sendFromQueueThread = Thread.ofVirtual().start(this::sendFromQueue);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        final Promise<RecordMetadata> promise = Promise.promise();
        if (isClosed.get()) {
            promise.fail("Producer is closed");
        } else {
            eventQueue.add(new RecordPromise<K, V>(record, promise));
        }
        return promise.future();
    }

    private void sendFromQueue() {
        while (!isClosed.get() || !eventQueue.isEmpty()) {
            try {
                RecordPromise<K, V> recordPromise = eventQueue.take();
                ProducerTracer.StartedSpan startedSpan =
                        this.tracer == null ? null : this.tracer.prepareSendMessage(ctx, recordPromise.getRecord());

                recordPromise
                        .getPromise()
                        .future()
                        .onComplete(v -> {
                            if (startedSpan != null) {
                                startedSpan.finish(ctx);
                            }
                        })
                        .onFailure(cause -> {
                            if (startedSpan != null) {
                                startedSpan.fail(ctx, cause);
                            }
                        });
                try {
                    producer.send(recordPromise.getRecord(), (metadata, exception) -> {
                        if (exception != null) {
                            recordPromise.getPromise().fail(exception);
                        }
                        recordPromise.getPromise().complete(metadata);
                    });
                } catch (final KafkaException exception) {
                    recordPromise.getPromise().fail(exception);
                }
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for event queue to be populated.");
                break;
            }
        }
        logger.debug("Background thread finish.");
    }

    @Override
    public Future<Void> close() {
        final Promise<Void> promise = Promise.promise();
        this.isClosed.set(true);
        Thread.ofVirtual().start(() -> {
            try {
                while (!eventQueue.isEmpty()) {
                    Thread.sleep(2000L);
                }
                sendFromQueueThread.interrupt();
                sendFromQueueThread.join();
                producer.close();
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        });
        return promise.future();
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
        private final ProducerRecord<K, V> record;
        private final Promise<RecordMetadata> promise;

        private RecordPromise(ProducerRecord<K, V> record, Promise<RecordMetadata> promise) {
            this.record = record;
            this.promise = promise;
        }

        public ProducerRecord<K, V> getRecord() {
            return record;
        }

        public Promise<RecordMetadata> getPromise() {
            return promise;
        }
    }

    // Function needed for testing
    public boolean isSendFromQueueThreadAlive() {
        return sendFromQueueThread.isAlive();
    }

    public int getEventQueueSize() {
        return eventQueue.size();
    }
}
