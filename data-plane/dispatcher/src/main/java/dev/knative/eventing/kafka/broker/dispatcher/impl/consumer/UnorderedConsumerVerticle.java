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

import dev.knative.eventing.kafka.broker.dispatcher.DeliveryOrder;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link io.vertx.core.Verticle} implements an unordered consumer logic, as described in {@link DeliveryOrder#UNORDERED}.
 */
public final class UnorderedConsumerVerticle extends ConsumerVerticle {

    private static final Logger logger = LoggerFactory.getLogger(UnorderedConsumerVerticle.class);

    private static final long BACKOFF_DELAY_MS = 200;
    // This shouldn't be more than 2000, which is the default max time allowed
    // to block a verticle thread.
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final AtomicBoolean closed;
    private final AtomicInteger inFlightRecords;
    private final AtomicBoolean isPollInFlight;

    private final PartitionRevokedHandler partitionRevokedHandler;

    private final Map<TopicPartition, Long> lastOffsets;

    public UnorderedConsumerVerticle(final ConsumerVerticleContext context, final Initializer initializer) {
        super(context, initializer);
        this.inFlightRecords = new AtomicInteger(0);
        this.closed = new AtomicBoolean(false);
        this.isPollInFlight = new AtomicBoolean(false);
        this.lastOffsets = new ConcurrentHashMap<>();

        this.partitionRevokedHandler = partitions -> {
            for (final TopicPartition partition : partitions) {
                lastOffsets.remove(partition);
            }
            return Future.succeededFuture();
        };
    }

    @Override
    void startConsumer(final Promise<Void> startPromise) {
        Objects.requireNonNull(getConsumerRebalanceListener());

        this.consumer
                .subscribe(
                        Set.copyOf(getConsumerVerticleContext().getResource().getTopicsList()),
                        getConsumerRebalanceListener())
                .onFailure(startPromise::tryFail)
                .onSuccess(startPromise::tryComplete)
                .onSuccess(v -> poll());
    }

    /**
     * Vert.x auto-subscribe and handling of records might grow
     * unbounded, and it is particularly evident when the consumer
     * is slow to consume messages.
     * <p>
     * To apply backpressure, we need to bound the number of outbound
     * in-flight requests, so we need to manually poll for new records
     * as we dispatch them to the subscriber service.
     * <p>
     * The maximum number of outbound in-flight requests is already configurable
     * with the consumer parameter `max.poll.records`, and it's critical to
     * control the memory consumption of the dispatcher.
     */
    private synchronized void poll() {
        if (closed.get() || isPollInFlight.get()) {
            return;
        }
        if (inFlightRecords.get() >= getConsumerVerticleContext().getMaxPollRecords()) {
            logger.debug(
                    "In flight records exceeds " + ConsumerConfig.MAX_POLL_RECORDS_CONFIG
                            + " waiting for response from subscriber before polling for new records {} {} {}",
                    keyValue(
                            ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                            getConsumerVerticleContext().getMaxPollRecords()),
                    keyValue("records", inFlightRecords),
                    getConsumerVerticleContext().getLoggingKeyValue());
            return;
        }
        if (this.isPollInFlight.compareAndSet(false, true)) {
            this.consumer
                    .poll(POLL_TIMEOUT)
                    .onSuccess(records -> vertx.runOnContext(v -> this.handleRecords(records)))
                    .onFailure(cause -> {
                        if (cause instanceof WakeupException) {
                            return; // Do nothing we're shutting down
                        }

                        isPollInFlight.set(false);
                        logger.error(
                                "Failed to poll messages {}",
                                getConsumerVerticleContext().getLoggingKeyValue(),
                                cause);
                        // Wait before retrying.
                        vertx.setTimer(BACKOFF_DELAY_MS, t -> poll());
                    });
        }
    }

    private void handleRecords(final ConsumerRecords<Object, CloudEvent> records) {
        if (closed.get()) {
            isPollInFlight.compareAndSet(true, false);
            return;
        }

        // We are not forcing the dispatcher to send less than `max.poll.records`
        // requests because we don't want to keep records in-memory by waiting
        // for responses.
        this.inFlightRecords.addAndGet(records.count());
        isPollInFlight.compareAndSet(true, false);

        for (var record : records) {

            final var topicPartition = new TopicPartition(record.topic(), record.partition());

            final List<Future<Void>> recordDispatcherFutures = new ArrayList<>();

            // Handle skipped offsets first, we dispatch an OffsetSkippingCloudEvent in stead of the missing offsets
            if (lastOffsets.containsKey(topicPartition)) {
                for (long skipOffset = lastOffsets.get(topicPartition) + 1;
                        skipOffset < record.offset();
                        skipOffset++) {
                    recordDispatcherFutures.add(this.recordDispatcher.dispatch(new ConsumerRecord<>(
                            record.topic(), record.partition(), skipOffset, null, new OffsetSkippingCloudEvent())));
                }
            }

            lastOffsets.put(topicPartition, record.offset());

            recordDispatcherFutures.add(this.recordDispatcher.dispatch(record));

            Future.all(recordDispatcherFutures).onComplete(v -> {
                this.inFlightRecords.decrementAndGet();
                poll();
            });
        }

        poll();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        this.closed.set(true);
        // Stop the consumer
        super.stop(stopPromise);
    }

    @Override
    public PartitionRevokedHandler getPartitionRevokedHandler() {
        return partitionRevokedHandler;
    }
}
