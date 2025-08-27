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

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import dev.knative.eventing.kafka.broker.core.filter.Filter;
import dev.knative.eventing.kafka.broker.core.observability.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.observability.tracing.kafka.ConsumerTracer;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEvent;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.KafkaConsumerRecordUtils;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the core algorithm of the Dispatcher (see {@link
 * RecordDispatcherImpl#dispatch(ConsumerRecord)}).
 *
 * @see RecordDispatcherImpl#dispatch(ConsumerRecord)
 */
public class RecordDispatcherImpl implements RecordDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(RecordDispatcherImpl.class);

    private static final CloudEventDeserializer cloudEventDeserializer = new CloudEventDeserializer();

    // Invalid cloud event records that are discarded by dispatch may not have a record type. So we set Tag as below.
    private static final Tag INVALID_EVENT_TYPE_TAG = Tag.of(Metrics.Tags.EVENT_TYPE, "InvalidCloudEvent");

    private static final String KN_ERROR_DEST_EXT_NAME = "knativeerrordest";
    private static final String KN_ERROR_CODE_EXT_NAME = "knativeerrorcode";
    private static final String KN_ERROR_DATA_EXT_NAME = "knativeerrordata";
    private static final String EKB_ERROR_PREFIX = "kne-";
    private static final int KN_ERROR_DATA_MAX_BYTES = 1024;

    private final Filter filter;
    private final Function<ConsumerRecord<Object, CloudEvent>, Future<HttpResponse<?>>> subscriberSender;
    private final Function<ConsumerRecord<Object, CloudEvent>, Future<HttpResponse<?>>> dlsSender;
    private final RecordDispatcherListener recordDispatcherListener;
    private final AsyncCloseable closeable;
    private final ConsumerTracer consumerTracer;
    private final MeterRegistry meterRegistry;
    private final ConsumerVerticleContext consumerVerticleContext;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger inFlightEvents = new AtomicInteger(0);
    private final Promise<Void> closePromise = Promise.promise();

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
            final ConsumerVerticleContext consumerVerticleContext,
            final Filter filter,
            final CloudEventSender subscriberSender,
            final CloudEventSender deadLetterSinkSender,
            final ResponseHandler responseHandler,
            final RecordDispatcherListener recordDispatcherListener,
            final ConsumerTracer consumerTracer,
            final MeterRegistry meterRegistry) {
        Objects.requireNonNull(consumerVerticleContext, "provide consumerVerticleContext");
        Objects.requireNonNull(filter, "provide filter");
        Objects.requireNonNull(subscriberSender, "provide subscriberSender");
        Objects.requireNonNull(deadLetterSinkSender, "provide deadLetterSinkSender");
        Objects.requireNonNull(recordDispatcherListener, "provide offsetStrategy");
        Objects.requireNonNull(responseHandler, "provide sinkResponseHandler");

        this.consumerVerticleContext = consumerVerticleContext;
        this.filter = filter;
        this.subscriberSender = composeSenderAndSinkHandler(subscriberSender, responseHandler, "subscriber");
        this.dlsSender = composeSenderAndSinkHandler(deadLetterSinkSender, responseHandler, "dead letter sink");
        this.recordDispatcherListener = recordDispatcherListener;
        this.closeable = AsyncCloseable.compose(
                subscriberSender, deadLetterSinkSender, recordDispatcherListener, responseHandler);
        this.consumerTracer = consumerTracer;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Handle the given record and returns a future that completes when the dispatch is completed and the offset is committed
     *
     * @param record record to handle.
     */
    @Override
    public Future<Void> dispatch(ConsumerRecord<Object, CloudEvent> record) {
        if (closed.get()) {
            return Future.failedFuture("Dispatcher closed");
        }

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
        final var recordContext = new ConsumerRecordContext(record);

        if (record.value() instanceof InvalidCloudEvent) {
            incrementDiscardedRecord(recordContext);
            final var msg = String.format(
                    "Invalid record received topic %s, partition %d, offset %d",
                    record.topic(), record.partition(), record.offset());
            logger.error(msg);
            recordDispatcherListener.recordReceived(record);
            recordDispatcherListener.recordDiscarded(record);
            return Future.failedFuture(msg);
        }

        try {
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
            incrementDiscardedRecord(recordContext);
            logError("Exception occurred, discarding the record", record, ex);
            recordDispatcherListener.recordReceived(record);
            recordDispatcherListener.recordDiscarded(record);
            return Future.failedFuture(ex);
        }
    }

    private void onRecordReceived(final ConsumerRecordContext recordContext, Promise<Void> finalProm) {
        recordReceived(recordContext);

        // Trace record received event
        // This needs to be done manually in order to work properly with both ordered and unordered delivery.
        Context context = Vertx.currentContext();
        final ConsumerTracer.StartedSpan span = getStartedSpan(recordContext, context);
        finalProm.future().onComplete(ar -> {
            recordHandlingCompleted(recordContext);
            if (span != null) {
                if (ar.succeeded()) {
                    span.finish(context);
                } else {
                    span.fail(context, ar.cause());
                }
            }
        });

        recordDispatcherListener.recordReceived(recordContext.getRecord());
        // Execute filtering
        final var pass = filter.test(recordContext.getRecord().value());

        if (meterRegistry != null) {
            Metrics.eventProcessingLatency(getTags(recordContext))
                    .register(meterRegistry)
                    .record(recordContext.performLatency());
        }

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
        subscriberSender
                .apply(recordContext.getRecord())
                .onSuccess(response -> onSubscriberSuccess(response, recordContext, finalProm))
                .onFailure(ex -> onSubscriberFailure(ex, recordContext, finalProm));
    }

    private void onFilterNotMatching(final ConsumerRecordContext recordContext, final Promise<Void> finalProm) {
        logDebug("Record did not match filtering", recordContext.getRecord());
        recordDispatcherListener.recordDiscarded(recordContext.getRecord());
        finalProm.complete();
    }

    private void onSubscriberSuccess(
            final HttpResponse<?> response, final ConsumerRecordContext recordContext, final Promise<Void> finalProm) {
        logDebug("Successfully sent event to subscriber", recordContext.getRecord());

        recordDispatchLatency(response, recordContext);

        recordDispatcherListener.successfullySentToSubscriber(recordContext.getRecord());
        finalProm.complete();
    }

    private void onSubscriberFailure(
            final Throwable failure, final ConsumerRecordContext recordContext, final Promise<Void> finalProm) {

        var response = getResponse(failure);
        recordDispatchLatency(response, recordContext);

        if (response == null && failure instanceof ResponseFailureException) {
            response = ((ResponseFailureException) failure).getResponse();
        }

        // enhance event with extension attributes prior to forwarding to the dead letter sink
        final var transformedRecordContext = errorTransform(recordContext, response);

        dlsSender
                .apply(transformedRecordContext.getRecord())
                .onSuccess(v -> onDeadLetterSinkSuccess(transformedRecordContext, finalProm))
                .onFailure(ex -> onDeadLetterSinkFailure(transformedRecordContext, ex, finalProm));
    }

    private ConsumerRecordContext errorTransform(
            final ConsumerRecordContext recordContext, @Nullable final HttpResponse<?> response) {
        final var destination = consumerVerticleContext.getEgress().getDestination();
        if (response == null) {
            // if response is null we still want to add destination
            return addExtensions(recordContext, Map.of(KN_ERROR_DEST_EXT_NAME, destination));
        }

        final var extensions = new HashMap<String, String>();
        extensions.put(KN_ERROR_DEST_EXT_NAME, destination);
        extensions.put(KN_ERROR_CODE_EXT_NAME, String.valueOf(response.statusCode()));

        // match for prefixed headers and put them to our extensions map
        response.headers().forEach((k, v) -> {
            if (k.regionMatches(true, 0, EKB_ERROR_PREFIX, 0, EKB_ERROR_PREFIX.length())) { // aka startsWithIgnoreCase
                extensions.put(k.substring(EKB_ERROR_PREFIX.length()).toLowerCase(), v);
            }
        });

        // we extract the response as byte array as we do not need a string
        // representation of it
        var data = response.bodyAsBuffer();
        if (data != null) {
            if (data.length() > KN_ERROR_DATA_MAX_BYTES) {
                data = Buffer.buffer(data.getBytes(0, KN_ERROR_DATA_MAX_BYTES));
            }
            extensions.put(KN_ERROR_DATA_EXT_NAME, Base64.getEncoder().encodeToString(data.getBytes()));
        }

        return addExtensions(recordContext, extensions);
    }

    // creates a new instance of ConsumerRecordContext with added extension attributes for underlying CloudEvent
    private ConsumerRecordContext addExtensions(
            final ConsumerRecordContext recordContext, final Map<String, String> extensions) {
        final var cloudEvent = recordContext.getRecord().value();

        final var builder = CloudEventBuilder.v1(cloudEvent);
        extensions.forEach(builder::withExtension);
        final var transformedCloudEvent = builder.build();

        final var cr = new ConsumerRecord<>(
                recordContext.getRecord().topic(),
                recordContext.getRecord().partition(),
                recordContext.getRecord().offset(),
                recordContext.getRecord().timestamp(),
                recordContext.getRecord().timestampType(),
                recordContext.getRecord().serializedKeySize(),
                recordContext.getRecord().serializedValueSize(),
                recordContext.getRecord().key(),
                transformedCloudEvent,
                recordContext.getRecord().headers(),
                recordContext.getRecord().leaderEpoch());

        return new ConsumerRecordContext(cr);
    }

    private void onDeadLetterSinkSuccess(final ConsumerRecordContext recordContext, final Promise<Void> finalProm) {
        logDebug("Successfully sent event to the dead letter sink", recordContext.getRecord());
        recordDispatcherListener.successfullySentToDeadLetterSink(recordContext.getRecord());
        finalProm.complete();
    }

    private void onDeadLetterSinkFailure(
            final ConsumerRecordContext recordContext, final Throwable exception, final Promise<Void> finalProm) {
        recordDispatcherListener.failedToSendToDeadLetterSink(recordContext.getRecord(), exception);
        finalProm.complete();
    }

    private ConsumerRecordContext maybeDeserializeValueFromHeaders(ConsumerRecordContext recordContext) {
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
        final var value = cloudEventDeserializer.deserialize(
                recordContext.getRecord().topic(), recordContext.getRecord().headers(), (byte[]) null);
        recordContext.setRecord(KafkaConsumerRecordUtils.copyRecordAssigningValue(recordContext.getRecord(), value));
        return recordContext;
    }

    private Function<ConsumerRecord<Object, CloudEvent>, Future<HttpResponse<?>>> composeSenderAndSinkHandler(
            CloudEventSender sender, ResponseHandler sinkHandler, String senderType) {
        return rec -> sender.send(rec.value())
                .onFailure(ex -> logError("Failed to send event to " + senderType, rec, ex))
                .compose(res -> sinkHandler
                        .handle(res)
                        .onFailure(ex -> logError("Failed to handle " + senderType + " response", rec, ex))
                        .map(v -> res));
    }

    private void incrementDiscardedRecord(final ConsumerRecordContext recordContext) {
        if (meterRegistry != null) {
            Metrics.discardedEventCount(getTags(recordContext))
                    .register(meterRegistry)
                    .increment();
        }
    }

    private void recordDispatchLatency(final HttpResponse<?> response, final ConsumerRecordContext recordContext) {
        final var latency = recordContext.performLatency();
        logger.debug(
                "Dispatch latency {} {}", consumerVerticleContext.getLoggingKeyValue(), keyValue("latency", latency));

        if (meterRegistry != null) {
            Metrics.eventDispatchLatency(getTags(response, recordContext))
                    .register(meterRegistry)
                    .record(latency);
        }
    }

    private HttpResponse<?> getResponse(final Throwable throwable) {
        for (Throwable c = throwable; c != null; c = c.getCause()) {
            if (c instanceof ResponseFailureException) {
                return ((ResponseFailureException) c).getResponse();
            }
        }
        return null;
    }

    private Tags getTags(HttpResponse<?> response, ConsumerRecordContext recordContext) {
        Tags tags;
        if (response == null) {
            tags = Tags.of(
                    Metrics.Tags.EVENT_TYPE, recordContext.getRecord().value().getType());
        } else {
            tags = this.consumerVerticleContext
                    .getTags()
                    .and(
                            Tag.of(Metrics.Tags.RESPONSE_CODE, Integer.toString(response.statusCode())),
                            Tag.of(
                                    Metrics.Tags.EVENT_TYPE,
                                    recordContext.getRecord().value().getType()));
        }
        return tags;
    }

    private Tags getTags(final ConsumerRecordContext recordContext) {
        if (recordContext.getRecord().value() instanceof InvalidCloudEvent) {
            return this.consumerVerticleContext.getTags().and(INVALID_EVENT_TYPE_TAG);
        }
        return this.consumerVerticleContext
                .getTags()
                .and(Tag.of(
                        Metrics.Tags.EVENT_TYPE,
                        recordContext.getRecord().value().getType()));
    }

    private void logError(final String msg, final ConsumerRecord<Object, CloudEvent> record, final Throwable cause) {

        if (logger.isDebugEnabled()) {
            logger.error(
                    msg + " {} {} {} {} {} {}",
                    consumerVerticleContext.getLoggingKeyValue(),
                    keyValue("topic", record.topic()),
                    keyValue("partition", record.partition()),
                    keyValue("offset", record.offset()),
                    keyValue("headers", record.headers()),
                    keyValue("event", record.value()),
                    cause);
        } else {
            logger.error(
                    msg + " {} {} {} {}",
                    consumerVerticleContext.getLoggingKeyValue(),
                    keyValue("topic", record.topic()),
                    keyValue("partition", record.partition()),
                    keyValue("offset", record.offset()),
                    cause);
        }
    }

    private void logDebug(final String msg, final ConsumerRecord<Object, CloudEvent> record) {

        logger.debug(
                msg + " {} {} {} {} {} {} {}",
                consumerVerticleContext.getLoggingKeyValue(),
                keyValue("topic", record.topic()),
                keyValue("partition", record.partition()),
                keyValue("headers", record.headers()),
                keyValue("offset", record.offset()),
                keyValue("key", record.key()),
                keyValue("event", record.value()));
    }

    private ConsumerTracer.StartedSpan getStartedSpan(ConsumerRecordContext recordContext, Context context) {
        if (consumerTracer != null) {
            return consumerTracer.prepareMessageReceived(context, recordContext.getRecord());
        }
        return null;
    }

    private void recordHandlingCompleted(final ConsumerRecordContext recordContext) {
        logDebug("Record handling completed", recordContext.getRecord());
        inFlightEvents.decrementAndGet();
        if (closed.get() && inFlightEvents.get() == 0) {
            closePromise.tryComplete();
        }
    }

    private void recordReceived(final ConsumerRecordContext recordContext) {
        logDebug("Handling record", recordContext.getRecord());
        inFlightEvents.incrementAndGet();
    }

    public Future<Void> close() {
        this.closed.set(true);

        Metrics.searchEgressMeters(
                        meterRegistry, consumerVerticleContext.getEgress().getReference())
                .forEach(meterRegistry::remove);

        if (inFlightEvents.get() == 0) {
            closePromise.tryComplete();
        }

        return closePromise
                .future()
                .compose(v -> this.closeable.close(), v -> this.closeable.close())
                .onComplete(
                        r -> logger.info("Record dispatcher closed {}", consumerVerticleContext.getLoggingKeyValue()));
    }
}
