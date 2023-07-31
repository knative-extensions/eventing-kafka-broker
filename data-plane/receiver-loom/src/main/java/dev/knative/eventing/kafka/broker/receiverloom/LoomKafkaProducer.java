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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class LoomKafkaProducer<K, V> implements ReactiveKafkaProducer<K, V> {

    private final Producer<K, V> producer;

    private final BlockingQueue<RecordPromise> eventQueue;
    private final AtomicBoolean isClosed;
    private final ProducerTracer tracer;
    private final VertxInternal vertx;
    private final ContextInternal ctx;
    private final Thread sendFromQueueThread;

    public LoomKafkaProducer(Vertx v, Producer<K, V> producer) {
        this.producer = producer;
        this.eventQueue = new LinkedBlockingQueue<>();
        this.isClosed = new AtomicBoolean(false);
        this.vertx = (VertxInternal) v;

        if (v != null) {
            ContextInternal ctxInt = ((ContextInternal) v.getOrCreateContext()).unwrap();
            this.tracer = ProducerTracer.create(ctxInt.tracer());
            this.ctx = vertx.getOrCreateContext();
        } else {
            this.tracer = null;
            this.ctx = null;
        }
        sendFromQueueThread = Thread.ofVirtual().start(this::sendFromQueue);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        Promise<RecordMetadata> promise = Promise.promise();
        if (isClosed.get()) {
            promise.fail("Producer is closed");
        } else {
            eventQueue.add(new RecordPromise(record, promise));
        }
        return promise.future();
    }

    private void sendFromQueue() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                RecordPromise recordPromise = eventQueue.take();
                ProducerTracer.StartedSpan startedSpan =
                        this.tracer == null ? null : this.tracer.prepareSendMessage(ctx, recordPromise.getRecord());
                try {
                    var metadata = producer.send(recordPromise.getRecord());
                    recordPromise.getPromise().complete(metadata.get());
                    if (startedSpan != null) {
                        startedSpan.finish(ctx);
                    }
                } catch (Exception e) {
                    recordPromise.getPromise().fail(e);
                    if (startedSpan != null) {
                        startedSpan.fail(ctx, e);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Future<Void> close() {
        Promise<Void> promise = Promise.promise();
        this.isClosed.set(true);
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(2000L);
                sendFromQueueThread.interrupt();
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
        Promise<Void> promise = Promise.promise();
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

    private class RecordPromise {
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
}
