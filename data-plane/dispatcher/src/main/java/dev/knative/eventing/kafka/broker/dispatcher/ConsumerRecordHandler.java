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
package dev.knative.eventing.kafka.broker.dispatcher;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import io.vertx.core.CompositeFuture;
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
 * @see ConsumerRecordHandler#handle(KafkaConsumerRecord)
 */
public final class ConsumerRecordHandler implements Handler<KafkaConsumerRecord<String, CloudEvent>> {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerRecordHandler.class);

  private final Filter filter;
  private final ConsumerRecordSender subscriberSender;
  private final ConsumerRecordSender deadLetterQueueSender;
  private final ConsumerRecordOffsetStrategy receiver;
  private final SinkResponseHandler sinkResponseHandler;

  /**
   * All args constructor.
   *
   * @param subscriberSender      sender to trigger subscriber
   * @param filter                event filter
   * @param receiver              hook receiver {@link ConsumerRecordOffsetStrategy}. It allows to plug in custom offset
   *                              management depending on the success/failure during the algorithm.
   * @param sinkResponseHandler   handler of the response from {@code subscriberSender}
   * @param deadLetterQueueSender sender to DLQ
   */
  public ConsumerRecordHandler(
    final ConsumerRecordSender subscriberSender,
    final Filter filter,
    final ConsumerRecordOffsetStrategy receiver,
    final SinkResponseHandler sinkResponseHandler,
    final ConsumerRecordSender deadLetterQueueSender) {

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
   * @param receiver            hook receiver {@link ConsumerRecordOffsetStrategy}. It allows to plug in custom offset
   *                            management depending on the success/failure during the algorithm.
   * @param sinkResponseHandler handler of the response
   */
  public ConsumerRecordHandler(
    final ConsumerRecordSender subscriberSender,
    final Filter filter,
    final ConsumerRecordOffsetStrategy receiver,
    final SinkResponseHandler sinkResponseHandler) {

    this(
      subscriberSender,
      filter,
      receiver,
      sinkResponseHandler,
      // If there is no DLQ configured by default DLQ sender always fails, which means
      // implementors will receive failedToSendToDLQ if the subscriber sender fails.
      ConsumerRecordSender.create(Future.failedFuture("No DLQ configured"), Future.succeededFuture())
    );
  }

  /**
   * Handle the given record.
   *
   * @param record record to handle.
   */
  @Override
  public void handle(final KafkaConsumerRecord<String, CloudEvent> record) {

    logDebug("Handling record", record);

    // TODO Maybe translate all this dispatching logic to a cool FSM
    receiver.recordReceived(record);

    if (filter.test(record.value())) {
      logDebug("Record match filtering", record);
      send(record);
    } else {
      logDebug("Record doesn't match filtering", record);
      receiver.recordDiscarded(record);
    }
  }

  private void send(final KafkaConsumerRecord<String, CloudEvent> record) {
    subscriberSender.send(record)
      .onSuccess(response -> sinkResponseHandler.handle(response)
        .onSuccess(ignored -> {
          logDebug("Successfully send response to the broker", record);
          receiver.successfullySentToSubscriber(record);
        })
        .onFailure(cause -> {
          logError("Failed to handle response", record, cause);
          sendToDLS(record);
        }))
      .onFailure(cause -> {
        logError("Failed to send event to subscriber", record, cause);
        sendToDLS(record);
      });
  }

  private void sendToDLS(KafkaConsumerRecord<String, CloudEvent> record) {
    deadLetterQueueSender.send(record)
      .onFailure(ex -> {
        logError("Failed to send record to dead letter sink", record, ex);
        receiver.failedToSendToDLQ(record, ex);
      })
      .onSuccess(response -> sinkResponseHandler.handle(response)
        .onSuccess(ignored -> {
          logDebug("Successfully send response to the broker", record);
          receiver.successfullySentToDLQ(record);
        })
        .onFailure(cause -> {
          logError("Failed to handle response", record, cause);
          receiver.failedToSendToDLQ(record, cause);
        }));
  }

  private static void logError(
    final String msg,
    final KafkaConsumerRecord<String, CloudEvent> record,
    final Throwable cause) {

    if (logger.isDebugEnabled()) {
      logger.error(msg + " {} {} {} {} {}",
        keyValue("topic", record.topic()),
        keyValue("partition", record.partition()),
        keyValue("headers", record.headers()),
        keyValue("offset", record.offset()),
        keyValue("event", record.value()),
        cause
      );
    } else {
      logger.error(msg + " {} {} {}",
        keyValue("topic", record.topic()),
        keyValue("partition", record.partition()),
        keyValue("offset", record.offset()),
        cause
      );
    }
  }

  private static void logDebug(
    final String msg,
    final KafkaConsumerRecord<String, CloudEvent> record) {

    logger.debug(msg + " {} {} {} {} {}",
      keyValue("topic", record.topic()),
      keyValue("partition", record.partition()),
      keyValue("headers", record.headers()),
      keyValue("offset", record.offset()),
      keyValue("event", record.value())
    );
  }

  public Future<?> close() {
    return CompositeFuture.all(
      this.sinkResponseHandler.close(),
      this.deadLetterQueueSender.close(),
      this.subscriberSender.close()
    );
  }
}
