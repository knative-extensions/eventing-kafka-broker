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
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class LoomKafkaProducer<K, V> implements ReactiveKafkaProducer<K, V> {

    private final KafkaProducer<K, V> producer;

    private final Queue<RecordPromise> queue;
    private final AtomicBoolean isRunning;

    public LoomKafkaProducer(KafkaProducer<K, V> producer) {
        this.producer = producer;
        this.queue = new ConcurrentLinkedQueue<>();
        this.isRunning = new AtomicBoolean(false);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        Promise<RecordMetadata> promise = Promise.promise();
        queue.add(new RecordPromise(record, promise));
        if (isRunning.compareAndSet(false, true)) {
            Thread.ofVirtual().start(this::sendFromQueue).setPriority(Thread.MAX_PRIORITY);
        }
        return promise.future();
    }

    private void sendFromQueue() {
        while (!queue.isEmpty()) {
            RecordPromise recordPromise = queue.poll();
            try {
                producer.send(recordPromise.getRecord(), (metadata, exception) -> {
                    if (exception != null) {
                        recordPromise.getPromise().fail(exception);
                    } else {
                        recordPromise.getPromise().complete(metadata);
                    }
                });
            } catch (Exception e) {
                recordPromise.getPromise().fail(e);
            }
        }
        isRunning.set(false);
    }

    @Override
    public Future<Void> close() {
        Promise<Void> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
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
