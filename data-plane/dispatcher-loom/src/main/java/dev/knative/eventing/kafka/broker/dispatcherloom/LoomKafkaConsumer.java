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
package dev.knative.eventing.kafka.broker.dispatcherloom;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class LoomKafkaConsumer<K, V> implements ReactiveKafkaConsumer<K, V> {

    private final KafkaConsumer<K, V> consumer;

    private Handler<Throwable> exceptionHandler;

    public LoomKafkaConsumer(Vertx vertx, Map<String, Object> configs) {
        this.consumer = new KafkaConsumer<>(configs);
    }

    @Override
    public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offset) {
        Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
                consumer.commitAsync(offset, (offsetMap, exception) -> {
                    if(exception != null) {
                        if(this.exceptionHandler != null) {
                            this.exceptionHandler.handle(exception);
                        }
                        promise.fail(exception);
                    } else {
                        promise.complete(offsetMap);
                    }
                });
            } catch (Exception e) {
                if(this.exceptionHandler != null) {
                    this.exceptionHandler.handle(e);
                }
                promise.fail(e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> close() {
        Promise<Void> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
                consumer.close();
                promise.complete();
            } catch (Exception e) {
                if(this.exceptionHandler != null) {
                    this.exceptionHandler.handle(e);
                }
                promise.fail(e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> pause(Collection<TopicPartition> partitions) {
        Promise<Void> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
                consumer.pause(partitions);
                promise.complete();
            } catch (Exception e) {
                if(this.exceptionHandler != null) {
                    this.exceptionHandler.handle(e);
                }
                promise.fail(e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<ConsumerRecords<K, V>> poll(Duration timeout) {
        Promise<ConsumerRecords<K, V>> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
                ConsumerRecords<K, V> records = consumer.poll(timeout);
                promise.complete(records);
            } catch (Exception e) {
                if(this.exceptionHandler != null) {
                    this.exceptionHandler.handle(e);
                }
                promise.fail(e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> resume(Collection<TopicPartition> partitions) {
        Promise<Void> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
                consumer.resume(partitions);
                promise.complete();
            } catch (Exception e) {
                if(this.exceptionHandler != null) {
                    this.exceptionHandler.handle(e);
                }
                promise.fail(e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> subscribe(Collection<String> topics) {
        Promise<Void> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
                consumer.subscribe(topics);
                promise.complete();
            } catch (Exception e) {
                if(this.exceptionHandler != null) {
                    this.exceptionHandler.handle(e);
                }
                promise.fail(e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        Promise<Void> promise = Promise.promise();
        Thread.ofVirtual().start(() -> {
            try {
                consumer.subscribe(topics, listener);
                promise.complete();
            } catch (Exception e) {
                if(this.exceptionHandler != null) {
                    this.exceptionHandler.handle(e);
                }
                promise.fail(e);
            }
        });
        return promise.future();
    }

    @Override
    public Consumer<K, V> unwrap() {
        return this.consumer;
    }

    @Override
    public ReactiveKafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }
    
}
