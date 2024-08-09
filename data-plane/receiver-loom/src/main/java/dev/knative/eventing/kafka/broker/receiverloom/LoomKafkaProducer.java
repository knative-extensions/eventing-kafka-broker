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
import io.vertx.core.tracing.TracingPolicy;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
    private final ProducerTracer<?> tracer;
    private final VertxInternal vertx;
    private final Thread sendFromQueueThread;
    private final Promise<Void> closePromise = Promise.promise();

    public LoomKafkaProducer(Vertx v, Producer<K, V> producer) {
        Objects.requireNonNull(v, "Vertx cannot be null");
        this.producer = producer;
        this.eventQueue = new LinkedBlockingQueue<>();
        this.isClosed = new AtomicBoolean(false);
        this.vertx = (VertxInternal) v;
        final var ctxInt = ((ContextInternal) v.getOrCreateContext()).unwrap();
        if (ctxInt.tracer() != null) {
            this.tracer =
                    new ProducerTracer(ctxInt.tracer(), TracingPolicy.PROPAGATE, "" /* TODO add bootrstrap servers */);
        } else {
            this.tracer = null;
        }

        if (Boolean.parseBoolean(System.getenv("ENABLE_VIRTUAL_THREADS"))) {
            this.sendFromQueueThread = Thread.ofVirtual().start(this::sendFromQueue);
        } else {
            this.sendFromQueueThread = new Thread(this::sendFromQueue);
            this.sendFromQueueThread.start();
        }
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        final Promise<RecordMetadata> promise = Promise.promise();
        if (isClosed.get()) {
            promise.fail("Producer is closed");
        } else {
            eventQueue.add(new RecordPromise<>(record, this.vertx.getOrCreateContext(), promise));
        }
        return promise.future();
    }

    private void sendFromQueue() {
        // Process queue elements until this is closed and the tasks queue is empty
        while (!isClosed.get() || !eventQueue.isEmpty()) {
            try {
                final var recordPromise = eventQueue.poll(2000, TimeUnit.MILLISECONDS);
                if (recordPromise == null) {
                    continue;
                }

                final var startedSpan = this.tracer == null
                        ? null
                        : this.tracer.prepareSendMessage(recordPromise.getContext(), recordPromise.getRecord());

                recordPromise
                        .getPromise()
                        .future()
                        .onComplete(v -> {
                            if (startedSpan != null) {
                                startedSpan.finish(recordPromise.getContext());
                            }
                        })
                        .onFailure(cause -> {
                            if (startedSpan != null) {
                                startedSpan.fail(recordPromise.getContext(), cause);
                            }
                        });
                try {
                    producer.send(
                            recordPromise.getRecord(),
                            (metadata, exception) -> recordPromise.getContext().runOnContext(v -> {
                                if (exception != null) {
                                    recordPromise.getPromise().fail(exception);
                                    return;
                                }
                                recordPromise.getPromise().complete(metadata);
                            }));
                } catch (final KafkaException exception) {
                    recordPromise
                            .getContext()
                            .runOnContext(v -> recordPromise.getPromise().fail(exception));
                }
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for event queue to be populated.");
                break;
            }
        }

        logger.debug("Background thread completed.");
    }

    @Override
    public Future<Void> close() {
        if (!this.isClosed.compareAndSet(false, true)) {
            return closePromise.future();
        }

        logger.debug("Closing producer");

        Thread.ofVirtual().start(() -> {
            try {
                while (!eventQueue.isEmpty()) {
                    logger.debug("Waiting for the eventQueue to become empty");
                    Thread.sleep(2000L);
                }
                logger.debug("Waiting for sendFromQueueThread thread to complete");
                sendFromQueueThread.join();
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
        private final ProducerRecord<K, V> record;
        private final ContextInternal context;
        private final Promise<RecordMetadata> promise;

        private RecordPromise(ProducerRecord<K, V> record, ContextInternal context, Promise<RecordMetadata> promise) {
            this.record = record;
            this.context = context;
            this.promise = promise;
        }

        public ProducerRecord<K, V> getRecord() {
            return record;
        }

        public Promise<RecordMetadata> getPromise() {
            return promise;
        }

        public ContextInternal getContext() {
            return context;
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
