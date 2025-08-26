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
package dev.knative.eventing.kafka.broker.core;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.observability.metrics.Metrics;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.vertx.core.Future;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;

/**
 * This executor performs an ordered execution of the enqueued tasks.
 * <p>
 * This class assumes its execution is tied to a single verticle, hence it cannot be shared among verticles.
 */
public class OrderedAsyncExecutor {

    private final Queue<Task> queue;

    private final AtomicBoolean isStopped;
    private final AtomicBoolean inFlight;
    private final MeterRegistry meterRegistry;
    private final DistributionSummary executorLatency;
    private final Gauge executorQueueLength;
    private final DataPlaneContract.Egress egress;

    public OrderedAsyncExecutor(
            final TopicPartition topicPartition,
            final MeterRegistry meterRegistry,
            final DataPlaneContract.Egress egress) {
        this.meterRegistry = meterRegistry;
        this.queue = new ArrayDeque<>();
        this.isStopped = new AtomicBoolean(false);
        this.inFlight = new AtomicBoolean(false);
        this.egress = egress;

        if (meterRegistry != null && egress != null && egress.getFeatureFlags().getEnableOrderedExecutorMetrics()) {
            final var tags = Tags.of(
                            Metrics.Tags.PARTITION_ID, topicPartition.partition() + "",
                            Metrics.Tags.DESTINATION_NAME, topicPartition.topic(),
                            Metrics.Tags.CONSUMER_NAME, egress.getReference().getName())
                    .and(Metrics.resourceRefTags(egress.getReference()));
            this.executorLatency = Metrics.executorQueueLatency(tags).register(meterRegistry);
            this.executorQueueLength =
                    Metrics.queueLength(tags, this.queue::size).register(meterRegistry);
        } else {
            this.executorLatency = null;
            this.executorQueueLength = null;
        }
    }

    /**
     * Offer a new task to the executor. The executor will start the task as soon as possible.
     *
     * @param task the task to offer
     */
    public void offer(Supplier<Future<?>> task) {
        if (this.isStopped.get()) {
            // Executor is stopped, return without adding the task to the queue.
            return;
        }
        boolean wasEmpty = this.queue.isEmpty();
        this.queue.offer(new Task(task));
        if (egress != null && egress.getFeatureFlags().getEnableOrderedExecutorMetrics()) {
            this.executorQueueLength.value();
        }
        if (wasEmpty) { // If no elements in the queue, then we need to start consuming it
            consume();
        }
    }

    private void consume() {
        if (queue.isEmpty() || this.inFlight.get() || this.isStopped.get()) {
            return;
        }
        if (this.inFlight.compareAndSet(false, true)) {
            final var task = this.queue.remove();
            task.task.get().onComplete(ar -> {
                this.inFlight.set(false);
                if (egress != null
                        && egress.getFeatureFlags().getEnableOrderedExecutorMetrics()
                        && !this.isStopped.get()) {
                    this.executorLatency.record(System.currentTimeMillis() - task.queueTimestamp);
                    this.executorQueueLength.value();
                }
                consume();
            });
        }
    }

    public boolean isWaitingForTasks() {
        // TODO To perform this flag would be nice to take into account also the time that it takes for a sink to
        // process a
        //  message so that we can fetch records in advance and keep queues busy.
        return this.queue.isEmpty();
    }

    /**
     * Stop the executor. This won't stop the actual in-flight task, but it will prevent queued tasks to be executed.
     */
    public void stop() {
        this.isStopped.set(true);
        this.queue.clear();
        if (meterRegistry != null && egress != null && egress.getFeatureFlags().getEnableOrderedExecutorMetrics()) {
            this.meterRegistry.remove(executorLatency);
            this.meterRegistry.remove(executorQueueLength);
        }
    }

    private static final class Task {

        private final Supplier<Future<?>> task;
        private final long queueTimestamp = System.currentTimeMillis();

        Task(final Supplier<Future<?>> task) {
            this.task = task;
        }
    }
}
