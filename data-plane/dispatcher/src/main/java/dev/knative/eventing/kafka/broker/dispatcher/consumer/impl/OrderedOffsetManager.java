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

import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import io.vertx.core.Future;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

/**
 * This class implements the offset strategy for the ordered consumer.
 */
public final class OrderedOffsetManager implements RecordDispatcherListener {

  private static final Logger logger = LoggerFactory
    .getLogger(OrderedOffsetManager.class);

  private final KafkaConsumer<?, ?> consumer;

  private final Consumer<Integer> onCommit;

  /**
   * All args constructor.
   *
   * @param consumer Kafka consumer.
   * @param onCommit Callback invoked when an offset is actually committed
   */
  public OrderedOffsetManager(final KafkaConsumer<?, ?> consumer, final Consumer<Integer> onCommit) {
    Objects.requireNonNull(consumer, "provide consumer");

    this.consumer = consumer;
    this.onCommit = onCommit;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public Future<Void> recordReceived(final KafkaConsumerRecord<?, ?> record) {
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
  public Future<Void> successfullySentToDeadLetterSink(final KafkaConsumerRecord<?, ?> record) {
    return commit(record);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<Void> failedToSendToDeadLetterSink(final KafkaConsumerRecord<?, ?> record, final Throwable ex) {
    return Future.succeededFuture();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<Void> recordDiscarded(final KafkaConsumerRecord<?, ?> record) {
    return Future.succeededFuture();
  }

  private Future<Void> commit(final KafkaConsumerRecord<?, ?> record) {
    // Execute the actual commit
    return consumer.commit(Map.of(
      new TopicPartition(record.topic(), record.partition()),
      new OffsetAndMetadata(record.offset() + 1, ""))
    )
      .onSuccess(ignored -> {
        if (onCommit != null) {
          onCommit.accept(1);
        }
        logger.debug(
          "committed {} {} {}",
          keyValue("topic", record.topic()),
          keyValue("partition", record.partition()),
          keyValue("offset", record.offset() + 1)
        );
      })
      .onFailure(cause ->
        logger.error(
          "failed to commit {} {} {}",
          keyValue("topic", record.topic()),
          keyValue("partition", record.partition()),
          keyValue("offset", record.offset() + 1),
          cause
        )
      ).mapEmpty();
  }

}
