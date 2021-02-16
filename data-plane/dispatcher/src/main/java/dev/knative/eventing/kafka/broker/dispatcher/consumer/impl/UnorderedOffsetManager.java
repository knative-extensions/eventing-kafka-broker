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
package dev.knative.eventing.kafka.broker.dispatcher.consumer.impl;

import dev.knative.eventing.kafka.broker.dispatcher.consumer.OffsetManager;
import io.vertx.core.Future;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the offset strategy that makes sure that, even unordered, the offset commit is ordered.
 */
public final class UnorderedOffsetManager implements OffsetManager {

  private static final Logger logger = LoggerFactory
    .getLogger(UnorderedOffsetManager.class);

  private final KafkaConsumer<?, ?> consumer;

  // This map contains the last acked message for every known TopicPartition
  private final Map<TopicPartition, Long> lastAckedPerPartition;
  // This map contains the set of messages waiting to be acked for every known TopicPartition
  // The algorithm will commit the offset "o = set.last() + 1" when:
  //   set.first() == lastAckedOffset + 1 && set.last() - set.first() == set.size() - 1
  private final Map<TopicPartition, SortedSet<Long>> pendingAcksPerPartition;

  private final Consumer<Integer> onCommit;

  /**
   * All args constructor.
   *
   * @param consumer Kafka consumer.
   * @param onCommit Callback invoked when an offset is actually committed
   */
  public UnorderedOffsetManager(final KafkaConsumer<?, ?> consumer, final Consumer<Integer> onCommit) {
    Objects.requireNonNull(consumer, "provide consumer");

    this.consumer = consumer;
    this.lastAckedPerPartition = new HashMap<>();
    this.pendingAcksPerPartition = new HashMap<>();
    this.onCommit = onCommit;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public Future<Void> recordReceived(final KafkaConsumerRecord<?, ?> record) {
    // un-ordered processing doesn't require pause/resume lifecycle.

    // Because recordReceived is guaranteed to be called in order,
    // we use it to set the last seen acked offset.
    // TODO If this assumption doesn't work, use this.consumer.committed(new TopicPartition(record.topic(), record.partition()))
    this.lastAckedPerPartition.putIfAbsent(new TopicPartition(record.topic(), record.partition()), record.offset() - 1);
    return Future.succeededFuture();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<Void> successfullySentToSubscriber(final KafkaConsumerRecord<?, ?> record) {
    return commit(record);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<Void> successfullySentToDLQ(final KafkaConsumerRecord<?, ?> record) {
    return commit(record);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<Void> failedToSendToDLQ(final KafkaConsumerRecord<?, ?> record, final Throwable ex) {
    mutateStateAndCheckAck(new TopicPartition(record.topic(), record.partition()), record.offset());
    return Future.succeededFuture();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<Void> recordDiscarded(final KafkaConsumerRecord<?, ?> record) {
    mutateStateAndCheckAck(new TopicPartition(record.topic(), record.partition()), record.offset());
    return Future.succeededFuture();
  }

  /**
   * @return null if it shouldn't ack, otherwise the offset to ack - 1.
   */
  private Long mutateStateAndCheckAck(TopicPartition topicPartition, long offset) {
    long lastAckedOffset = this.lastAckedPerPartition.get(topicPartition); // This is always non null
    SortedSet<Long> partitionPendingAcks =
      this.pendingAcksPerPartition.computeIfAbsent(topicPartition, v -> new TreeSet<>());
    partitionPendingAcks.add(offset);

    // Return the ack to commit if:
    // * last acked offset is the same of the first pending ack in the set
    // * the set contains every element in its range of values
    return (partitionPendingAcks.first() == lastAckedOffset + 1 &&
      partitionPendingAcks.last() - partitionPendingAcks.first() == partitionPendingAcks.size() - 1) ?
      partitionPendingAcks.last() : null;
  }

  private Future<Void> commit(final KafkaConsumerRecord<?, ?> record) {
    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
    Long toAck = mutateStateAndCheckAck(topicPartition, record.offset());
    if (toAck != null) {
      // Reset the state
      this.lastAckedPerPartition.put(topicPartition, toAck);
      SortedSet<Long> messagesImGoingToAck = this.pendingAcksPerPartition.remove(topicPartition);

      // Execute the actual commit
      return consumer.commit(Map.of(
        topicPartition,
        new OffsetAndMetadata(toAck + 1, ""))
      )
        .onSuccess(ignored -> {
          if (onCommit != null) {
            onCommit.accept(messagesImGoingToAck.size());
          }
          logger.debug(
            "committed for topic partition {} {} offset {}",
            record.topic(),
            record.partition(),
            toAck + 1
          );
        })
        .onFailure(cause ->
          logger.error(
            "failed to commit for topic partition {} {} offset {}",
            record.topic(),
            record.partition(),
            toAck + 1,
            cause
          )
        ).mapEmpty();
    }
    return Future.succeededFuture();
  }

}
