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
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

import dev.knative.eventing.kafka.broker.core.OrderedAsyncExecutor;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;
import io.cloudevents.CloudEvent;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import io.github.bucket4j.local.LocalBucketBuilder;
import io.github.bucket4j.local.SynchronizationStrategy;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderedConsumerVerticle extends ConsumerVerticle {

    private static final Logger logger = LoggerFactory.getLogger(OrderedConsumerVerticle.class);

    private static final long POLLING_MS = 200L;
    private static final Duration POLLING_TIMEOUT = Duration.ofMillis(1000L);

    private final Map<TopicPartition, OrderedAsyncExecutor> recordDispatcherExecutors;
    private final Map<TopicPartition, Long> lastOffsets;
    private final PartitionRevokedHandler partitionRevokedHandler;
    private final Bucket bucket;

    private final AtomicBoolean closed;
    private final AtomicLong pollTimer;
    private final AtomicBoolean isPollInFlight;

    public OrderedConsumerVerticle(final ConsumerVerticleContext context, final Initializer initializer) {
        super(context, initializer);

        final var vReplicas = Math.max(1, context.getEgress().getVReplicas());
        final var tokens = context.getMaxPollRecords() * vReplicas;
        if (context.getEgress().getFeatureFlags().getEnableRateLimiter()) {
            this.bucket = new LocalBucketBuilder()
                    .addLimit(Bandwidth.classic(tokens, Refill.greedy(tokens, Duration.ofSeconds(1))))
                    .withSynchronizationStrategy(SynchronizationStrategy.SYNCHRONIZED)
                    .withMillisecondPrecision()
                    .build();
        } else {
            this.bucket = null;
        }

        this.recordDispatcherExecutors = new ConcurrentHashMap<>();
        this.lastOffsets = new ConcurrentHashMap<>();
        this.closed = new AtomicBoolean(false);
        this.isPollInFlight = new AtomicBoolean(false);
        this.pollTimer = new AtomicLong(-1);

        partitionRevokedHandler = partitions -> {
            // Stop executors associated with revoked partitions.
            for (final TopicPartition partition : partitions) {
                lastOffsets.remove(partition);
                final var executor = recordDispatcherExecutors.remove(partition);
                if (executor != null) {
                    logger.info(
                            "Stopping executor {} {}",
                            getConsumerVerticleContext().getLoggingKeyValue(),
                            keyValue("topicPartition", partition));
                    executor.stop();
                }
            }
            return Future.succeededFuture();
        };
    }

    @Override
    void startConsumer(Promise<Void> startPromise) {
        Objects.requireNonNull(getConsumerRebalanceListener());
        // We need to sub first, then we can start the polling loop
        this.consumer
                .subscribe(
                        Set.copyOf(getConsumerVerticleContext().getResource().getTopicsList()),
                        getConsumerRebalanceListener())
                .onFailure(startPromise::fail)
                .onSuccess(v -> {
                    if (this.pollTimer.compareAndSet(-1, 0)) {
                        this.pollTimer.set(vertx.setPeriodic(POLLING_MS, x -> poll()));
                    }
                    this.poll();
                    startPromise.complete();
                });
    }

    private void poll() {
        if (this.closed.get() || this.isPollInFlight.get()) {
            logger.debug(
                    "Consumer closed or poll is in-flight {}",
                    getConsumerVerticleContext().getLoggingKeyValue());
            return;
        }

        // When we don't have tokens available, we just wait `POLLING_MS`
        // before trying to poll again.
        if (bucket != null && bucket.getAvailableTokens() <= 0) {
            logger.info(
                    "Rate limiter, tokens unavailable {} {}",
                    keyValue("wait.ms", POLLING_MS),
                    getConsumerVerticleContext().getLoggingKeyValue());
            return;
        }

        // Only poll new records when at-least one internal per-partition queue
        // needs more records.
        if (areAllExecutorsBusy()) {
            logger.debug(
                    "all executors are busy {}", getConsumerVerticleContext().getLoggingKeyValue());
            return;
        }

        if (this.isPollInFlight.compareAndSet(false, true)) {
            logger.debug("Polling for records {}", getConsumerVerticleContext().getLoggingKeyValue());

            this.consumer
                    .poll(POLLING_TIMEOUT)
                    .onSuccess(records -> vertx.runOnContext(v -> this.recordsHandler(records)))
                    .onFailure(t -> {
                        if (this.closed.get() || t instanceof WakeupException) {
                            // The failure might have been caused by stopping the consumer, so we just ignore it
                            return;
                        }
                        isPollInFlight.set(false);
                        exceptionHandler(t);
                    });
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        // Stop the executors
        this.closed.set(true);
        this.vertx.cancelTimer(this.pollTimer.get());
        this.recordDispatcherExecutors.values().forEach(OrderedAsyncExecutor::stop);
        // Stop the consumer
        super.stop(stopPromise);
    }

    @Override
    public PartitionRevokedHandler getPartitionRevokedHandler() {
        return partitionRevokedHandler;
    }

    private void recordsHandler(ConsumerRecords<Object, CloudEvent> records) {
        if (this.closed.get()) {
            return;
        }
        isPollInFlight.set(false);

        if (records == null || records.count() == 0) {
            return;
        }

        if (bucket != null) {
            // Once we have new records, we force add them to internal per-partition queues.
            bucket.forceAddTokens(records.count());
        }

        // Put records in internal per-partition queues.
        for (var record : records) {
            final var topicPartition = new TopicPartition(record.topic(), record.partition());

            final var executor = executorFor(topicPartition);

            // Handle skipped offsets first, we dispatch an OffsetSkippingCloudEvent in stead of the missing offsets
            if (lastOffsets.containsKey(topicPartition)) {
                for (long skipOffset = lastOffsets.get(topicPartition) + 1;
                        skipOffset < record.offset();
                        skipOffset++) {
                    final long skipOffsetFinal = skipOffset;
                    executor.offer(() -> dispatch(new ConsumerRecord<>(
                            record.topic(),
                            record.partition(),
                            skipOffsetFinal,
                            null,
                            new OffsetSkippingCloudEvent())));
                }
            }

            lastOffsets.put(topicPartition, record.offset());

            executor.offer(() -> dispatch(record));
        }
    }

    private Future<Void> dispatch(final ConsumerRecord<Object, CloudEvent> record) {
        if (this.closed.get()) {
            return Future.failedFuture("Consumer verticle closed " + getConsumerVerticleContext());
        }

        return this.recordDispatcher.dispatch(record);
    }

    private synchronized OrderedAsyncExecutor executorFor(final TopicPartition topicPartition) {
        var executor = this.recordDispatcherExecutors.get(topicPartition);
        if (executor != null) {
            return executor;
        }
        executor = new OrderedAsyncExecutor(
                topicPartition,
                getConsumerVerticleContext().getMetricsRegistry(),
                getConsumerVerticleContext().getEgress());
        this.recordDispatcherExecutors.put(topicPartition, executor);
        return executor;
    }

    private boolean areAllExecutorsBusy() {
        if (recordDispatcherExecutors.isEmpty()) {
            // No executors
            return false;
        }

        var res = true;
        final var toResume = new HashSet<TopicPartition>();
        final var toPause = new HashSet<TopicPartition>();

        for (var executor : recordDispatcherExecutors.entrySet()) {
            if (executor.getValue().isWaitingForTasks()) {
                toResume.add(executor.getKey());
                res = false;
            } else {
                toPause.add(executor.getKey());
            }
        }

        if (!toPause.isEmpty()) {
            this.consumer.pause(toPause);
        }
        if (!toResume.isEmpty()) {
            this.consumer.resume(toResume);
        }

        return res;
    }
}
