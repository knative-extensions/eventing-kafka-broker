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

import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

/**
 * This class implements the offset strategy that makes sure that, even unordered, the offset commit is ordered.
 */
public final class OffsetManager implements RecordDispatcherListener {

  private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);

  private final KafkaConsumer<?, ?> consumer;

  private final Map<TopicPartition, OffsetTracker> offsetTrackers;

  private final Consumer<Integer> onCommit;
  private final long timerId;
  private final Vertx vertx;

  /**
   * All args constructor.
   *
   * @param consumer Kafka consumer.
   * @param onCommit Callback invoked when an offset is actually committed
   */
  public OffsetManager(final Vertx vertx,
                       final KafkaConsumer<?, ?> consumer,
                       final Consumer<Integer> onCommit,
                       final long commitIntervalMs) {
    Objects.requireNonNull(consumer, "provide consumer");

    this.consumer = consumer;
    this.offsetTrackers = new HashMap<>();
    this.onCommit = onCommit;

    this.timerId = vertx.setPeriodic(commitIntervalMs, l -> commitAll());
    this.vertx = vertx;

    this.consumer.partitionsRevokedHandler(partitions -> {
      logPartitions("revoked", partitions);
      partitions.forEach(offsetTrackers::remove);
    });

    this.consumer.partitionsAssignedHandler(partitions ->
      partitions.forEach(tp -> logPartitions("assigned", partitions))
    );
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public void recordReceived(final KafkaConsumerRecord<?, ?> record) {
    final var tp = new TopicPartition(record.topic(), record.partition());
    if (!offsetTrackers.containsKey(tp)) {
      // Initialize offset tracker for the given record's topic/partition.
      offsetTrackers.putIfAbsent(tp, new OffsetTracker(record.offset()));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void successfullySentToSubscriber(final KafkaConsumerRecord<?, ?> record) {
    commit(record);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void successfullySentToDeadLetterSink(final KafkaConsumerRecord<?, ?> record) {
    commit(record);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void failedToSendToDeadLetterSink(final KafkaConsumerRecord<?, ?> record, final Throwable ex) {
    commit(record);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void recordDiscarded(final KafkaConsumerRecord<?, ?> record) {
    commit(record);
  }

  private void commit(final KafkaConsumerRecord<?, ?> record) {
    // We need to handle the case when the offset tracker was removed from our Map since when partitions are revoked we
    // remove the associated offset tracker, however, we may get a response from the sink for a previously owned
    // partition after a partition has been revoked.
    // Note: it's not possible to commit offsets of partitions that this a particular consumer instance doesn't own.
    final var ot = this.offsetTrackers.get(new TopicPartition(record.topic(), record.partition()));
    if (ot != null) {
      ot.recordNewOffset(record.offset());
    }
  }

  private synchronized Future<Void> commit(final TopicPartition topicPartition, final OffsetTracker tracker) {
    long newOffset = tracker.offsetToCommit();
    if (newOffset > tracker.getCommitted()) {
      // Reset the state
      tracker.setCommitted(newOffset);

      logger.debug("Committing offset for {} offset {}",
        keyValue("topicPartition", topicPartition),
        keyValue("offset", newOffset));

      // Execute the actual commit
      return consumer.commit(Map.of(topicPartition, new OffsetAndMetadata(newOffset, "")))
        .onSuccess(ignored -> {
          if (onCommit != null) {
            onCommit.accept((int) newOffset);
          }
        })
        .onFailure(cause -> logger.error("Failed to commit topic partition {} offset {}",
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
    return CompositeFuture.all(
      this.offsetTrackers.entrySet()
        .stream()
        .map(e -> commit(e.getKey(), e.getValue()))
        .filter(Objects::nonNull)
        .collect(Collectors.toList())
    ).mapEmpty();
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
  private static class OffsetTracker {

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
      // Since the delivery might be unordered we should copy the state of the current committedOffset BitSet that goes
      // from the committed offset to the end of the BitSet.

      final var prevCommittedOffsetsArr = committedOffsets.toLongArray();
      // Calculate the word index in the long array. Long size is 64.
      final var relativeOffset = committed - initialOffset;
      final var wordOfCommitted = (int) (relativeOffset / 64);

      // Copy from wordOfCommitted to the end: [..., wordOfCommitted, ...]
      final var newCommittedOffsetsArr = Arrays.copyOfRange(
        prevCommittedOffsetsArr,
        wordOfCommitted,
        prevCommittedOffsetsArr.length
      );

      // Re-create committedOffset BitSet and reset initialOffset.
      this.committedOffsets = BitSet.valueOf(newCommittedOffsetsArr);
      this.initialOffset = committed;
    }
  }

  private static void logPartitions(final String context,
                                    final Set<TopicPartition> tps) {
    logger.info("Partitions " + context + " {}", keyValue("partitions", tps));
  }
}
