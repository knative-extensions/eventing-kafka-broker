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
package dev.knative.eventing.kafka.broker.dispatcher.strategy;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategy;
import io.micrometer.core.instrument.Counter;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the offset strategy that makes sure that, even unordered, the offset commit is ordered.
 */
public final class UnorderedConsumerRecordOffsetStrategy implements ConsumerRecordOffsetStrategy {

  private static final Logger logger = LoggerFactory
    .getLogger(UnorderedConsumerRecordOffsetStrategy.class);

  private final Map<TopicPartition, Long> lastAckedMap;
  private final Map<TopicPartition, SortedSet<Long>> pendingAcksMap;
  private final KafkaConsumer<?, ?> consumer;
  private final Counter eventsSentCounter;

  /**
   * All args constructor.
   *
   * @param consumer          Kafka consumer.
   * @param eventsSentCounter events sent counter
   */
  public UnorderedConsumerRecordOffsetStrategy(final KafkaConsumer<?, ?> consumer, final Counter eventsSentCounter) {
    Objects.requireNonNull(consumer, "provide consumer");
    Objects.requireNonNull(eventsSentCounter, "provide eventsSentCounter");

    this.consumer = consumer;
    this.eventsSentCounter = eventsSentCounter;
    this.lastAckedMap = new HashMap<>();
    this.pendingAcksMap = new HashMap<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void recordReceived(final KafkaConsumerRecord<?, ?> record) {
    // un-ordered processing doesn't require pause/resume lifecycle.

    // Because recordReceived is guaranteed to be called in order,
    // we use it to set the last seen acked offset.
    // TODO If this assumption doesn't work, use this.consumer.committed(new TopicPartition(record.topic(), record.partition()))
    this.lastAckedMap.putIfAbsent(new TopicPartition(record.topic(), record.partition()), record.offset() - 1);
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
  public void successfullySentToDLQ(final KafkaConsumerRecord<?, ?> record) {
    commit(record);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void failedToSendToDLQ(final KafkaConsumerRecord<?, ?> record, final Throwable ex) {
    // TODO what do we do with that?
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void recordDiscarded(final KafkaConsumerRecord<?, ?> record) {
    commit(record);
  }

  private Long shouldAck(TopicPartition topicPartition, long offset) {
    long lastAckedOffset = this.lastAckedMap.get(topicPartition); // This is always non null
    SortedSet<Long> toAckSet = this.pendingAcksMap.computeIfAbsent(topicPartition, v -> new TreeSet<>());
    toAckSet.add(offset);

    return (toAckSet.first() == lastAckedOffset + 1 && toAckSet.last() - toAckSet.first() == toAckSet.size() - 1) ?
      toAckSet.last() : null;
  }

  private void commit(final KafkaConsumerRecord<?, ?> record) {
    TopicPartition key = new TopicPartition(record.topic(), record.partition());
    Long shouldAck = shouldAck(key, record.offset());
    if (shouldAck != null) {
      // Reset the state
      long lastAckedBeforeThisOne = this.lastAckedMap.put(key, shouldAck);
      SortedSet<Long> messagesImGoingToAck = this.pendingAcksMap.remove(key);

      // Execute the actual commit
      consumer.commit(Map.of(
        key,
        new OffsetAndMetadata(shouldAck + 1, ""))
      ).onSuccess(ignored -> {
        eventsSentCounter.increment(messagesImGoingToAck.size());
        logger.debug(
          "committed {} {} {}",
          keyValue("topic", record.topic()),
          keyValue("partition", record.partition()),
          keyValue("offset", shouldAck)
        );
      })
        .onFailure(cause -> {
          // If the commit failed, there are 2 situations:
          // * Nobody tried to commit again, so let's just restore the state
          // * Somebody committed with an offset greater than this one, so we just discard the error
          if (!(this.lastAckedMap.get(key) > shouldAck)) {
            this.lastAckedMap.put(key, lastAckedBeforeThisOne);
            this.pendingAcksMap.compute(key, (k, actual) -> {
              if (actual != null) {
                actual.addAll(messagesImGoingToAck);
                return actual;
              }
              return messagesImGoingToAck;
            });
          }
          logger.error(
            "failed to commit {} {} {}",
            keyValue("topic", record.topic()),
            keyValue("partition", record.partition()),
            keyValue("offset", shouldAck),
            cause
          );
        });
    }
  }

}
