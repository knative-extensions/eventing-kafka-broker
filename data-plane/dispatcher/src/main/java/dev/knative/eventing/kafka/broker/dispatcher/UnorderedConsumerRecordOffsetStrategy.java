/*
 * Copyright 2020 The Knative Authors
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

package dev.knative.eventing.kafka.broker.dispatcher;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UnorderedConsumerRecordOffsetStrategy<K, V> implements
    ConsumerRecordOffsetStrategy<K, V> {

  private static final Logger logger = LoggerFactory
      .getLogger(UnorderedConsumerRecordOffsetStrategy.class);

  private final KafkaConsumer<K, V> consumer;

  /**
   * All args constructor.
   *
   * @param consumer Kafka consumer.
   */
  public UnorderedConsumerRecordOffsetStrategy(final KafkaConsumer<K, V> consumer) {
    Objects.requireNonNull(consumer, "provide consumer");

    this.consumer = consumer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void recordReceived(final KafkaConsumerRecord<K, V> record) {
    // un-ordered processing doesn't require pause/resume lifecycle.
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void successfullySentToSubscriber(final KafkaConsumerRecord<K, V> record) {
    // TODO evaluate if it's worth committing offsets at specified intervals per partition.
    // commit each record
    commit(record)
        .onSuccess(ignored -> logger.debug(
            "committed {} {} {}",
            keyValue("topic", record.topic()),
            keyValue("partition", record.partition()),
            keyValue("offset", record.offset())
        ))
        .onFailure(cause -> logger.error(
            "failed to commit {} {} {}",
            keyValue("topic", record.topic()),
            keyValue("partition", record.partition()),
            keyValue("offset", record.offset()),
            cause
        ));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void successfullySentToDLQ(final KafkaConsumerRecord<K, V> record) {
    successfullySentToSubscriber(record);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void failedToSendToDLQ(final KafkaConsumerRecord<K, V> record, final Throwable ex) {
    // do not commit
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void recordDiscarded(final KafkaConsumerRecord<K, V> record) {
    successfullySentToSubscriber(record);
  }

  private Future<Map<TopicPartition, OffsetAndMetadata>> commit(
      final KafkaConsumerRecord<K, V> record) {

    logger.debug("committing record {}", record);

    final Promise<Map<TopicPartition, OffsetAndMetadata>> promise = Promise.promise();

    final var topicPartitionsToCommit = Map.of(
        topicPartition(record),
        new OffsetAndMetadata(record.offset() + 1, "")
    );

    consumer.commit(topicPartitionsToCommit, promise);

    return promise.future();
  }

  private static <K, V> TopicPartition topicPartition(final KafkaConsumerRecord<K, V> record) {
    return new TopicPartition(record.topic(), record.partition());
  }
}
