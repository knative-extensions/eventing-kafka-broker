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

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoomKafkaConsumer<K, V> implements ReactiveKafkaConsumer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(LoomKafkaConsumer.class);

    private final Consumer<K, V> consumer;
    private final BlockingQueue<Runnable> taskQueue;
    private final AtomicBoolean isClosed;
    private final Thread taskRunnerThread;
    private Handler<Throwable> exceptionHandler;

    public LoomKafkaConsumer(Vertx vertx, Consumer<K, V> consumer) {
        this.consumer = consumer;
        this.taskQueue = new LinkedBlockingQueue<>();
        this.isClosed = new AtomicBoolean(false);

        taskRunnerThread = Thread.ofVirtual().start(this::processTaskQueue);
    }

    private void addTask(Runnable task, Promise<?> promise) {
        if (isClosed.get()) {
            promise.fail("Consumer is closed");
        }
        taskQueue.add(task);
    }

    private void processTaskQueue() {
        while (!isClosed.get() || !taskQueue.isEmpty()) {
            try {
                taskQueue.take().run();
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for task", e);
            }
        }
    }

    @Override
    public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offset) {
        final Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();
        addTask(
                () -> {
                    consumer.commitAsync(offset, (offsetMap, exception) -> {
                        if (exception != null) {
                            promise.fail(exception);
                        } else {
                            promise.complete(offsetMap);
                        }
                    });
                },
                promise);
        return promise.future();
    }

    @Override
    public Future<Void> close() {
        final Promise<Void> promise = Promise.promise();
        isClosed.set(true);
        taskQueue.add(() -> {
            try {
                consumer.close();
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        });

        Thread.ofVirtual().start(() -> {
            try {
                while (!taskQueue.isEmpty()) {
                    Thread.sleep(100);
                }
                taskRunnerThread.interrupt();
                taskRunnerThread.join();
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for taskRunnerThread to finish", e);
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> pause(Collection<TopicPartition> partitions) {
        final Promise<Void> promise = Promise.promise();
        addTask(
                () -> {
                    try {
                        consumer.pause(partitions);
                        promise.complete();
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                },
                promise);
        return promise.future();
    }

    @Override
    public Future<ConsumerRecords<K, V>> poll(Duration timeout) {
        final Promise<ConsumerRecords<K, V>> promise = Promise.promise();
        addTask(
                () -> {
                    try {
                        ConsumerRecords<K, V> records = consumer.poll(timeout);
                        promise.complete(records);
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                },
                promise);
        return promise.future();
    }

    @Override
    public Future<Void> resume(Collection<TopicPartition> partitions) {
        final Promise<Void> promise = Promise.promise();
        addTask(
                () -> {
                    try {
                        consumer.resume(partitions);
                        promise.complete();
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                },
                promise);
        return promise.future();
    }

    @Override
    public Future<Void> subscribe(Collection<String> topics) {
        final Promise<Void> promise = Promise.promise();
        addTask(
                () -> {
                    try {
                        consumer.subscribe(topics);
                        promise.complete();
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                },
                promise);
        return promise.future();
    }

    @Override
    public Future<Void> subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        final Promise<Void> promise = Promise.promise();
        addTask(
                () -> {
                    try {
                        consumer.subscribe(topics, listener);
                        promise.complete();
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                },
                promise);
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
