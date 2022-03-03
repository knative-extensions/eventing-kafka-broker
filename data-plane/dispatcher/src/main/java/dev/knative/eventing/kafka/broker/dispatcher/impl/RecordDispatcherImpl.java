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
package dev.knative.eventing.kafka.broker.dispatcher.impl;

import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.KafkaConsumerRecordUtils;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.kafka.client.common.tracing.ConsumerTracer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

/**
 * This class implements the core algorithm of the Dispatcher (see {@link
 * RecordDispatcherImpl#dispatch(KafkaConsumerRecord)}).
 *
 * @see RecordDispatcherImpl#dispatch(KafkaConsumerRecord)
 */
public class RecordDispatcherImpl implements RecordDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(RecordDispatcherImpl.class);

  private static final CloudEventDeserializer cloudEventDeserializer = new CloudEventDeserializer();

  // When we don't have an HTTP response, due to other errors for networking, etc,
  // we only add the code class tag in the "error" class (5xx).
  private static final Tag NO_RESPONSE_CODE_CLASS_TAG = Tag.of(Metrics.Tags.RESPONSE_CODE_CLASS, "5xx");

  private final Filter filter;
  private final Function<KafkaConsumerRecord<Object, CloudEvent>, Future<HttpResponse<?>>> subscriberSender;
  private final Function<KafkaConsumerRecord<Object, CloudEvent>, Future<HttpResponse<?>>> dlsSender;
  private final RecordDispatcherListener recordDispatcherListener;
  private final AsyncCloseable closeable;
  private final ConsumerTracer consumerTracer;
  private final MeterRegistry meterRegistry;
  private final ResourceContext resourceContext;

  private final Tags noResponseResourceTags;

  /**
   * All args constructor.
   *
   * @param filter                   event filter
   * @param subscriberSender         sender to trigger subscriber
   * @param deadLetterSinkSender     sender to dead letter sink
   * @param responseHandler          handler of the response from {@code subscriberSender}
   * @param recordDispatcherListener hook receiver {@link RecordDispatcherListener}. It allows to plug in custom offset
   * @param consumerTracer           consumer tracer
   */
  public RecordDispatcherImpl(
    final ResourceContext resourceContext,
    final Filter filter,
    final CloudEventSender subscriberSender,
    final CloudEventSender deadLetterSinkSender,
    final ResponseHandler responseHandler,
    final RecordDispatcherListener recordDispatcherListener,
    final ConsumerTracer consumerTracer,
    final MeterRegistry meterRegistry) {
    Objects.requireNonNull(resourceContext, "provide resourceContext");
    Objects.requireNonNull(filter, "provide filter");
    Objects.requireNonNull(subscriberSender, "provide subscriberSender");
    Objects.requireNonNull(deadLetterSinkSender, "provide deadLetterSinkSender");
    Objects.requireNonNull(recordDispatcherListener, "provide offsetStrategy");
    Objects.requireNonNull(responseHandler, "provide sinkResponseHandler");
    Objects.requireNonNull(meterRegistry, "provide meterRegistry");

    this.resourceContext = resourceContext;
    this.filter = filter;
    this.subscriberSender = composeSenderAndSinkHandler(subscriberSender, responseHandler, "subscriber");
    this.dlsSender = composeSenderAndSinkHandler(deadLetterSinkSender, responseHandler, "dead letter sink");
    this.recordDispatcherListener = recordDispatcherListener;
    this.closeable = AsyncCloseable.compose(responseHandler, deadLetterSinkSender, subscriberSender, recordDispatcherListener);
    this.consumerTracer = consumerTracer;
    this.meterRegistry = meterRegistry;

    this.noResponseResourceTags = resourceContext.getTags().and(NO_RESPONSE_CODE_CLASS_TAG);
  }

  /**
   * Handle the given record and returns a future that completes when the dispatch is completed and the offset is committed
   *
   * @param record record to handle.
   */
  @Override
  public Future<Void> dispatch(KafkaConsumerRecord<Object, CloudEvent> record) {
    /*
    That's pretty much what happens here:

    +------------+    +------------+     +-------------+
    |            |    |            |     |             |
    |  record    |    |  filter    |     |  subscriber |
    |  received  +--->+  matching  +---->+  success    +-->end
    |            |    |            |     |             |
    +-+-------+--+    +-----+------+     +-----+-------+
      |       |             |                  |
      |       v             |                  |             +-------------+
      |    +--+---------+   |           +------v-------+     |             |
      |    |            |   |           |  subscriber  +---->+  dls        |
      |    | filter not |   +---------->+  failure     |     |  success    +---->end
      |    | matching   |               +----+---------+     |             |
      |    |            |                    |               +-----+-------+
      |    +---+--------+              +-----v-------+             |
      |        |                       | dls failure |             v
      |        |                       +-------------+----------> end
      +->end<--+
     */

    try {
      final var recordContext = new ConsumerRecordContext(record);
      Promise<Void> promise = Promise.promise();
      onRecordReceived(maybeDeserializeValueFromHeaders(recordContext), promise);
      return promise.future();
    } catch (final Exception ex) {
      // This is a fatal exception that shouldn't happen in normal cases.
      //
      // It might happen if folks send bad records to a topic that is
      // managed by our system.
      //
      // So discard record if we can't deal with the record, so that we can
      // make progress in the partition.
      logError("Exception occurred, discarding the record", record, ex);
      recordDispatcherListener.recordReceived(record);
      recordDispatcherListener.recordDiscarded(record);
      return Future.failedFuture(ex);
    }
  }

  private void onRecordReceived(final ConsumerRecordContext recordContext, Promise<Void> finalProm) {
    logDebug("Handling record", recordContext.getRecord());

    // Trace record received event
    // This needs to be done manually in order to work properly with both ordered and unordered delivery.
    if (consumerTracer != null) {
      Context context = Vertx.currentContext();
      ConsumerTracer.StartedSpan span = consumerTracer.prepareMessageReceived(context, recordContext.getRecord().record());
      if (span != null) {
        finalProm.future()
          .onComplete(ar -> {
            if (ar.succeeded()) {
              span.finish(context);
            } else {
              span.fail(context, ar.cause());
            }
          });
      }
    }

    recordDispatcherListener.recordReceived(recordContext.getRecord());
    // Execute filtering
    final var pass = filter.test(recordContext.getRecord().value());

    Metrics
      .eventProcessingLatency(getTags(recordContext))
      .register(meterRegistry)
      .record(recordContext.performLatency());

    // reset timer to start calculating the dispatch latency.
    recordContext.resetTimer();

    if (pass) {
      onFilterMatching(recordContext, finalProm);
    } else {
      onFilterNotMatching(recordContext, finalProm);
    }
  }

  private void onFilterMatching(final ConsumerRecordContext recordContext, final Promise<Void> finalProm) {
    logDebug("Record matched filtering", recordContext.getRecord());
    subscriberSender.apply(recordContext.getRecord())
      .onSuccess(response -> onSubscriberSuccess(response, recordContext, finalProm))
      .onFailure(ex -> onSubscriberFailure(ex, recordContext, finalProm));
  }

  private void onFilterNotMatching(final ConsumerRecordContext recordContext,
                                   final Promise<Void> finalProm) {
    logDebug("Record did not match filtering", recordContext.getRecord());
    recordDispatcherListener.recordDiscarded(recordContext.getRecord());
    finalProm.complete();
  }

  private void onSubscriberSuccess(final HttpResponse<?> response,
                                   final ConsumerRecordContext recordContext,
                                   final Promise<Void> finalProm) {
    logDebug("Successfully sent event to subscriber", recordContext.getRecord());

    incrementEventCount(response, recordContext);
    recordDispatchLatency(response, recordContext);

    recordDispatcherListener.successfullySentToSubscriber(recordContext.getRecord());
    finalProm.complete();
  }

  private void onSubscriberFailure(final Throwable failure,
                                   final ConsumerRecordContext recordContext,
                                   final Promise<Void> finalProm) {

    final var response = getResponse(failure);
    incrementEventCount(response, recordContext);
    recordDispatchLatency(response, recordContext);

    dlsSender.apply(recordContext.getRecord())
      .onSuccess(v -> onDeadLetterSinkSuccess(recordContext, finalProm))
      .onFailure(ex -> onDeadLetterSinkFailure(recordContext, ex, finalProm));
  }

  private void onDeadLetterSinkSuccess(final ConsumerRecordContext recordContext,
                                       final Promise<Void> finalProm) {
    logDebug("Successfully sent event to the dead letter sink", recordContext.getRecord());
    recordDispatcherListener.successfullySentToDeadLetterSink(recordContext.getRecord());
    finalProm.complete();
  }


  private void onDeadLetterSinkFailure(final ConsumerRecordContext recordContext,
                                       final Throwable exception,
                                       final Promise<Void> finalProm) {
    recordDispatcherListener.failedToSendToDeadLetterSink(recordContext.getRecord(), exception);
    finalProm.complete();
  }

  private static ConsumerRecordContext maybeDeserializeValueFromHeaders(ConsumerRecordContext recordContext) {
    if (recordContext.getRecord().value() != null) {
      return recordContext;
    }
    // A valid CloudEvent in the CE binary protocol binding of Kafka
    // might be composed by only Headers.
    //
    // KafkaConsumer doesn't call the deserializer if the value
    // is null.
    //
    // That means that we get a record with a null value and some CE
    // headers even though the record is a valid CloudEvent.
    logDebug("Value is null", recordContext.getRecord());
    final var value = cloudEventDeserializer.deserialize(recordContext.getRecord().record().topic(), recordContext.getRecord().record().headers(), null);
    recordContext.setRecord(new KafkaConsumerRecordImpl<>(KafkaConsumerRecordUtils.copyRecordAssigningValue(recordContext.getRecord().record(), value)));
    return recordContext;
  }

  private static Function<KafkaConsumerRecord<Object, CloudEvent>, Future<HttpResponse<?>>> composeSenderAndSinkHandler(
    CloudEventSender sender, ResponseHandler sinkHandler, String senderType) {
    return rec -> sender.send(rec.value())
      .onFailure(ex -> logError("Failed to send event to " + senderType, rec, ex))
      .compose(res ->
        sinkHandler.handle(res)
          .onFailure(ex -> logError("Failed to handle " + senderType + " response", rec, ex))
          .map(v -> res)
      );
  }

  private void incrementEventCount(@Nullable final HttpResponse<?> response,
                                   final ConsumerRecordContext recordContext) {
    Metrics
      .eventCount(getTags(response, recordContext))
      .register(meterRegistry)
      .increment();
  }

  private void recordDispatchLatency(final HttpResponse<?> response,
                                     final ConsumerRecordContext recordContext) {
    final var latency = recordContext.performLatency();
    logger.debug("Dispatch latency {}", keyValue("latency", latency));

    Metrics
      .eventDispatchLatency(getTags(response, recordContext))
      .register(meterRegistry)
      .record(latency);
  }

  private HttpResponse<?> getResponse(final Throwable throwable) {
    for (Throwable c = throwable; c != null; c = c.getCause()) {
      if (c instanceof ResponseFailureException) {
        final var response = (HttpResponse<?>) ((ResponseFailureException) c).getResponse();
        return response;
      }
    }
    return null;
  }

  private Tags getTags(HttpResponse<?> response, ConsumerRecordContext recordContext) {
    Tags tags;
    if (response == null) {
      tags = this.noResponseResourceTags.and(
        Tag.of(Metrics.Tags.EVENT_TYPE, recordContext.getRecord().value().getType())
      );
    } else {
      tags = this.resourceContext.getTags().and(
        Tag.of(Metrics.Tags.RESPONSE_CODE_CLASS, response.statusCode() / 100 + "xx"),
        Tag.of(Metrics.Tags.RESPONSE_CODE, Integer.toString(response.statusCode())),
        Tag.of(Metrics.Tags.EVENT_TYPE, recordContext.getRecord().value().getType())
      );
    }
    return tags;
  }

  private Tags getTags(final ConsumerRecordContext recordContext) {
    return this.resourceContext.getTags().and(
      Tag.of(Metrics.Tags.EVENT_TYPE, recordContext.getRecord().value().getType())
    );
  }

  private static void logError(
    final String msg,
    final KafkaConsumerRecord<Object, CloudEvent> record,
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
    final KafkaConsumerRecord<Object, CloudEvent> record) {

    logger.debug(msg + " {} {} {} {} {} {}",
      keyValue("topic", record.topic()),
      keyValue("partition", record.partition()),
      keyValue("headers", record.headers()),
      keyValue("offset", record.offset()),
      keyValue("key", record.key()),
      keyValue("event", record.value())
    );
  }

  public Future<Void> close() {
    return this.closeable.close();
  }
}
