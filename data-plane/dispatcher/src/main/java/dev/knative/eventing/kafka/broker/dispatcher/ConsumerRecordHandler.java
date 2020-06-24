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

import dev.knative.eventing.kafka.broker.core.Filter;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConsumerRecordHandler implements the core algorithm of the Dispatcher component (see {@link
 * ConsumerRecordHandler#handle(KafkaConsumerRecord)}).
 *
 * @param <K> type of records' key.
 * @param <V> type of records' value.
 * @param <R> type of the response of given senders.
 * @see ConsumerRecordHandler#handle(KafkaConsumerRecord)
 */
public final class ConsumerRecordHandler<K, V, R> implements
    Handler<KafkaConsumerRecord<K, V>> {

  private static final Logger logger = LoggerFactory
      .getLogger(ConsumerRecordHandler.class);

  private static final String SUBSCRIBER = "subscriber";
  private static final String DLQ = "dead letter queue";

  private final Filter<V> filter;
  private final ConsumerRecordSender<K, V, R> subscriberSender;
  private final ConsumerRecordSender<K, V, R> deadLetterQueueSender;
  private final ConsumerRecordOffsetStrategy<K, V> receiver;
  private final SinkResponseHandler<R> sinkResponseHandler;

  /**
   * All args constructor.
   *
   * @param subscriberSender      sender to trigger subscriber
   * @param filter                event filter
   * @param receiver              hook receiver {@link ConsumerRecordOffsetStrategy}. It allows to
   *                              plug in custom offset management depending on the success/failure
   *                              during the algorithm.
   * @param sinkResponseHandler   handler of the response from {@code subscriberSender}
   * @param deadLetterQueueSender sender to DLQ
   */
  public ConsumerRecordHandler(
      final ConsumerRecordSender<K, V, R> subscriberSender,
      final Filter<V> filter,
      final ConsumerRecordOffsetStrategy<K, V> receiver,
      final SinkResponseHandler<R> sinkResponseHandler,
      final ConsumerRecordSender<K, V, R> deadLetterQueueSender) {

    Objects.requireNonNull(filter, "provide filter");
    Objects.requireNonNull(subscriberSender, "provide subscriberSender");
    Objects.requireNonNull(deadLetterQueueSender, "provide deadLetterQueueSender");
    Objects.requireNonNull(receiver, "provider receiver");
    Objects.requireNonNull(sinkResponseHandler, "provider sinkResponseHandler");

    this.subscriberSender = subscriberSender;
    this.filter = filter;
    this.receiver = receiver;
    this.deadLetterQueueSender = deadLetterQueueSender;
    this.sinkResponseHandler = sinkResponseHandler;
  }

  /**
   * Call this constructor when there is no DLQ configured on the broker.
   *
   * @param subscriberSender    sender to trigger subscriber
   * @param filter              event filter
   * @param receiver            hook receiver {@link ConsumerRecordOffsetStrategy}. It allows to
   *                            plug in custom offset management depending on the success/failure
   *                            during the algorithm.
   * @param sinkResponseHandler handler of the response
   */
  public ConsumerRecordHandler(
      final ConsumerRecordSender<K, V, R> subscriberSender,
      final Filter<V> filter,
      final ConsumerRecordOffsetStrategy<K, V> receiver,
      final SinkResponseHandler<R> sinkResponseHandler) {

    this(
        subscriberSender,
        filter,
        receiver,
        sinkResponseHandler,
        // If there is no DLQ configured by default DLQ sender always fails, which means
        // implementors will receive failedToSendToDLQ if the subscriber sender fails.
        record -> Future.failedFuture("no DLQ configured")
    );
  }

  /**
   * Handle the given record.
   *
   * @param record record to handle.
   */
  @Override
  public void handle(final KafkaConsumerRecord<K, V> record) {

    receiver.recordReceived(record);

    if (filter.match(record.value())) {
      subscriberSender.send(record)
          .compose(sinkResponseHandler::handle)
          .onSuccess(response -> onSuccessfullySentToSubscriber(record))
          .onFailure(cause -> onFailedToSendToSubscriber(record, cause));
    } else {
      receiver.recordDiscarded(record);
    }
  }

  private void onSuccessfullySentToSubscriber(final KafkaConsumerRecord<K, V> record) {
    logSuccessfulSendTo(SUBSCRIBER, record);

    receiver.successfullySentToSubscriber(record);
  }

  private void onFailedToSendToSubscriber(
      final KafkaConsumerRecord<K, V> record,
      final Throwable cause) {

    logFailedSendTo(SUBSCRIBER, record, cause);

    deadLetterQueueSender.send(record)
        .compose(sinkResponseHandler::handle)
        .onSuccess(ignored -> onSuccessfullySentToDLQ(record))
        .onFailure(ex -> onFailedToSendToDLQ(record, ex));
  }

  private void onSuccessfullySentToDLQ(final KafkaConsumerRecord<K, V> record) {
    logSuccessfulSendTo(DLQ, record);

    receiver.successfullySentToDLQ(record);
  }

  private void onFailedToSendToDLQ(KafkaConsumerRecord<K, V> record, Throwable ex) {
    logFailedSendTo(DLQ, record, ex);

    receiver.failedToSendToDLQ(record, ex);
  }

  private static <K, V> void logFailedSendTo(
      final String component,
      final KafkaConsumerRecord<K, V> record,
      final Throwable cause) {

    logger.error(
        "{} sender failed to send record - topic: {} - partition: {} - offset: {} - cause: {}",
        component,
        record.topic(),
        record.partition(),
        record.offset(),
        cause
    );
  }

  private static <K, V> void logSuccessfulSendTo(
      final String component,
      final KafkaConsumerRecord<K, V> record) {

    if (logger.isDebugEnabled()) {
      logger.debug(
          "record successfully handled by {} - topic: {} - partition: {} - offset: {}",
          component,
          record.topic(),
          record.partition(),
          record.offset()
      );
    }
  }
}
