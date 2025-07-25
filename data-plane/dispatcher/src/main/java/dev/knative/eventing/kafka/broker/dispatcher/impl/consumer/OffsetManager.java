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

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the offset strategy that makes sure that, even unordered, the offset commit is ordered.
 */
public final class OffsetManager implements RecordDispatcherListener {

    private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);

    private final ReactiveKafkaConsumer<?, ?> consumer;

    private final Map<TopicPartition, OffsetTracker> offsetTrackers;

    private final Consumer<Integer> onCommit;
    private final long timerId;
    private final Vertx vertx;
    private final PartitionRevokedHandler partitionRevokedHandler;

    /**
     * All args constructor.
     *
     * @param consumer Kafka consumer.
     * @param onCommit Callback invoked when an offset is actually committed
     */
    public OffsetManager(
            final Vertx vertx,
            final ReactiveKafkaConsumer<?, ?> consumer,
            final Consumer<Integer> onCommit,
            final long commitIntervalMs) {
        Objects.requireNonNull(consumer, "provide consumer");

        this.consumer = consumer;
        this.offsetTrackers = new ConcurrentHashMap<>();
        this.onCommit = onCommit;

        this.timerId = vertx.setPeriodic(commitIntervalMs, l -> commitAll());
        this.vertx = vertx;

        partitionRevokedHandler = partitions -> {
            try {
                // Commit revoked partitions synchronously
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
                for (Map.Entry<TopicPartition, OffsetTracker> entry : offsetTrackers.entrySet()) {
                    TopicPartition topicPartition = entry.getKey();
                    if (partitions.contains(topicPartition)) {
                        OffsetTracker tracker = entry.getValue();
                        long newOffset = tracker.offsetToCommit();
                        if (newOffset > tracker.getCommitted()) {
                            offsetAndMetadataMap.put(topicPartition, new OffsetAndMetadata(newOffset, ""));
                            logger.debug(
                                    "Committing offset for {} offset {} from partition revoked handler",
                                    keyValue("topicPartition", topicPartition),
                                    keyValue("offset", newOffset));
                        }
                    }
                }

                if (!offsetAndMetadataMap.isEmpty()) {
                    // partition revoked handler is invoked from within poll, so this should be safe
                    consumer.unwrap().commitSync(offsetAndMetadataMap);

                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetAndMetadataMap.entrySet()) {
                        offsetTrackers
                                .get(entry.getKey())
                                .setCommitted(entry.getValue().offset());
                        if (onCommit != null) {
                            onCommit.accept((int) entry.getValue().offset());
                        }
                    }
                }

                // Remove revoked partitions.
                partitions.forEach(offsetTrackers::remove);
                return Future.succeededFuture();
            } catch (final Exception ex) {
                logger.warn("Failed to commit revoked partitions offsets {}", keyValue("partitions", partitions), ex);
                return Future.failedFuture(ex);
            } finally {
                // Remove revoked partitions in any case.
                partitions.forEach(offsetTrackers::remove);
                logPartitions("revoked", partitions);
            }
        };
    }

    public PartitionRevokedHandler getPartitionRevokedHandler() {
        return partitionRevokedHandler;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public void recordReceived(final ConsumerRecord<?, ?> record) {
        final var tp = new TopicPartition(record.topic(), record.partition());
        final var offsetTracker = offsetTrackers.putIfAbsent(tp, new OffsetTracker(record.offset()));
        if (offsetTracker != null && record.offset() < offsetTracker.initialOffset) {
            logger.debug(
                    "Received records offset ({}) is less than offsetTrackers initial offset ({})",
                    record.offset(),
                    offsetTracker.initialOffset);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void successfullySentToSubscriber(final ConsumerRecord<?, ?> record) {
        commit(record);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void successfullySentToDeadLetterSink(final ConsumerRecord<?, ?> record) {
        commit(record);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void failedToSendToDeadLetterSink(final ConsumerRecord<?, ?> record, final Throwable ex) {
        commit(record);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDiscarded(final ConsumerRecord<?, ?> record) {
        commit(record);
    }

    private void commit(final ConsumerRecord<?, ?> record) {
        // We need to handle the case when the offset tracker was removed from our Map since when partitions are revoked
        // we
        // remove the associated offset tracker, however, we may get a response from the sink for a previously owned
        // partition after a partition has been revoked.
        // Note: it's not possible to commit offsets of partitions that this a particular consumer instance doesn't own.
        final var ot = this.offsetTrackers.get(new TopicPartition(record.topic(), record.partition()));
        if (ot != null) {
            if (record.offset() < ot.initialOffset) {
                logger.debug(
                        "Ignoring commit for {}-{} offset {}, offset tracker already on {}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        ot.initialOffset);
                return;
            }

            ot.recordNewOffset(record.offset());
        }
    }

    private synchronized Future<Void> commit(final TopicPartition topicPartition, final OffsetTracker tracker) {
        long newOffset = tracker.offsetToCommit();
        if (newOffset > tracker.getCommitted()) {

            logger.debug(
                    "Committing offset for {} offset {}",
                    keyValue("topicPartition", topicPartition),
                    keyValue("offset", newOffset));

            // Execute the actual commit
            return consumer.commit(Map.of(topicPartition, new OffsetAndMetadata(newOffset, "")))
                    .onSuccess(ignored -> {
                        // Reset the state
                        tracker.setCommitted(newOffset);

                        if (onCommit != null) {
                            onCommit.accept((int) newOffset);
                        }
                    })
                    .onFailure(cause -> logger.error(
                            "Failed to commit topic partition {} offset {}",
                            keyValue("topicPartition", topicPartition),
                            keyValue("offset", newOffset),
                            cause))
                    .mapEmpty();
        }
        return null;
    }

    /**
     * Commit all tracked offsets by colling commit on every offsetTracker entry.
     *
     * @return succeeded or failed future.
     */
    private Future<Void> commitAll() {
        return CompositeFuture.all(this.offsetTrackers.entrySet().stream()
                        .map(e -> commit(e.getKey(), e.getValue()))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()))
                .mapEmpty();
    }

    /**
     * Commit all tracked offsets by colling commit on every offsetTracker entry.
     *
     * @return succeeded or failed future.
     */
    private Future<Void> commit(final Collection<TopicPartition> partitions) {
        return CompositeFuture.all(this.offsetTrackers.entrySet().stream()
                        .filter(kv -> partitions.contains(kv.getKey()))
                        .map(e -> commit(e.getKey(), e.getValue()))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()))
                .mapEmpty();
    }

    @Override
    public Future<Void> close() {
        vertx.cancelTimer(timerId);
        return commitAll();
    }

    /**
     * This offset tracker keeps track of the committed records for a
     * single partition.
     */
    static class OffsetTracker {

        /*
         * We use a BitSet as a replacement for an array of booleans.
         * Each bit represents an offset of a partition.
         *
         * A bit in the BitSet is set when an offset can be committed
         * to Kafka.
         *
         * Example case:
         *
         *  partition offsets   [0, 1, 2, 3, 4, 5, 6, 7, ... ]  <-- BitSet
         *  t1                                                  <-- time 1
         *  t1 success           ^     ^  ^        ^            <-- offsets recorded
         *  t1 to commit            &                           <-- offset to commit (next unset offset)
         *
         *  t2                                                  <-- time 2
         *  t2 success           ^  ^  ^  ^        ^            <-- offsets recorded
         *  t2 to commit                     &                  <-- offset to commit (next unset offset)
         */

        // In order to not use a huge amount of memory we cap the BitSet to a _dynamic_ max size governed by this
        // threshold.
        private static final int RESET_TRACKER_THRESHOLD = 1_000_000;

        // We store `long` offsets in a `BitSet` that is capable of handling `int` elements.
        // The BitSet sets a committed bit for an offset `offset` in this way:
        // bitSetOffset = offset - initialOffset
        private BitSet committedOffsets;

        // InitialOffset represents the offset committed from where committedOffsets BitSet starts, which means that
        // the state of committedOffsets[0] is equal to the state of partition[initialOffset].
        private long initialOffset;

        // CommittedOffsets is the actual offset committed to stable storage.
        private long committed;

        OffsetTracker(final long initialOffset) {
            committedOffsets = new BitSet();
            committed = Math.max(initialOffset, 0);
            this.initialOffset = committed;
        }

        synchronized void recordNewOffset(final long offset) {
            final var bitSetOffset = (int) (offset - initialOffset);
            committedOffsets.set(bitSetOffset);
            maybeReset(bitSetOffset);
        }

        synchronized long offsetToCommit() {
            return initialOffset + committedOffsets.nextClearBit((int) (committed - initialOffset));
        }

        synchronized void setCommitted(final long committed) {
            this.committed = committed;
        }

        synchronized long getCommitted() {
            return committed;
        }

        private void maybeReset(final int offset) {
            if (offset > RESET_TRACKER_THRESHOLD) {
                reset();
            }
        }

        private void reset() {
            // To not grow the BitSet indefinitely we create a new BitSet that starts from the committed offset.
            // Since the delivery might be unordered we should copy the state of the current committedOffset BitSet that
            // goes
            // from the committed offset to the end of the BitSet.

            final var prevCommittedOffsetsArr = committedOffsets.toLongArray();
            // Calculate the word index in the long array. Long size is 64.
            final var wordSize = 64;
            final var relativeOffset = committed - initialOffset;
            final var wordOfCommitted = (int) (relativeOffset / wordSize);

            // Copy from wordOfCommitted to the end: [..., wordOfCommitted, ...]
            final var newCommittedOffsetsArr =
                    Arrays.copyOfRange(prevCommittedOffsetsArr, wordOfCommitted, prevCommittedOffsetsArr.length);

            // Re-create committedOffset BitSet and reset initialOffset.
            this.committedOffsets = BitSet.valueOf(newCommittedOffsetsArr);
            this.initialOffset = committed - (relativeOffset % wordSize);
        }
    }

    private static void logPartitions(final String context, final Collection<TopicPartition> tps) {
        logger.info("Partitions " + context + " {}", keyValue("partitions", tps));
    }

    /* Visible for testing */
    Map<TopicPartition, OffsetTracker> getOffsetTrackers() {
        return offsetTrackers;
    }
}
