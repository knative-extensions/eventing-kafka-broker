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

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoomKafkaConsumer<K, V> implements ReactiveKafkaConsumer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(LoomKafkaConsumer.class);

    private final Consumer<K, V> consumer;
    private final BlockingQueue<Runnable> taskQueue;
    private final AtomicBoolean isClosed;
    private final Thread taskRunnerThread;
    private final Promise<Void> closePromise = Promise.promise();

    public LoomKafkaConsumer(Vertx vertx, Consumer<K, V> consumer) {
        this.consumer = consumer;
        this.taskQueue = new LinkedBlockingQueue<>();
        this.isClosed = new AtomicBoolean(false);

        if (Boolean.parseBoolean(System.getenv("ENABLE_VIRTUAL_THREADS"))) {
            this.taskRunnerThread = Thread.ofVirtual().start(this::processTaskQueue);
        } else {
            this.taskRunnerThread = new Thread(this::processTaskQueue);
            this.taskRunnerThread.start();
        }
    }

    private void addTask(Runnable task, Promise<?> promise) {
        if (isClosed.get()) {
            promise.fail("Consumer is closed");
            return;
        }
        taskQueue.add(task);
    }

    private void processTaskQueue() {
        // Process queue elements until this is closed and the tasks queue is empty
        while (!isClosed.get() || !taskQueue.isEmpty()) {
            try {
                Runnable task = taskQueue.poll(2000, TimeUnit.MILLISECONDS);
                if (task != null) {
                  task.run();
                }
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for task", e);
                break;
            }
        }
    }

    @Override
    public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offset) {
        final Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();
        addTask(
                () -> {
                    try {
                        consumer.commitSync(offset);
                        promise.complete(offset);
                    } catch (final KafkaException exception) {
                        promise.fail(exception);
                    }
                },
                promise);
        return promise.future();
    }

    @Override
    public Future<Void> close() {
        if (!this.isClosed.compareAndSet(false, true)) {
            return closePromise.future();
        }

        taskQueue.add(() -> {
            try {
                logger.debug("Closing underlying Kafka consumer client");
                consumer.wakeup();
                consumer.close();
            } catch (Exception e) {
                closePromise.tryFail(e);
            }
        });

        logger.debug("Closing consumer {}", keyValue("size", taskQueue.size()));

        Thread.ofVirtual().start(() -> {
            try {
                while (!taskQueue.isEmpty()) {
                    logger.debug("Queue is not empty {}", keyValue("taskQueue.size", taskQueue.size()));
                    Thread.sleep(2000L);
                }
                logger.debug("Queue is empty");

                taskRunnerThread.join();
                closePromise.tryComplete();

                logger.debug("Background thread completed");

            } catch (InterruptedException e) {
                final var size = taskQueue.size();
                logger.debug(
                        "Interrupted while waiting for taskRunnerThread to finish {}",
                        keyValue("taskQueueSize", size),
                        e);
                closePromise.tryFail(new InterruptedException("taskQueue.size = " + size + ". " + e.getMessage()));
            }
        });

        return closePromise.future();
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
        return this;
    }

    // functions needed for test
    public int getTaskQueueSize() {
        return taskQueue.size();
    }

    public boolean isTaskRunnerThreadAlive() {
        return taskRunnerThread.isAlive();
    }
}
