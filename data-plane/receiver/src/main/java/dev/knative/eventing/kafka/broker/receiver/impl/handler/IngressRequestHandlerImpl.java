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
package dev.knative.eventing.kafka.broker.receiver.impl.handler;

import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig;
import dev.knative.eventing.kafka.broker.core.tracing.TracingSpan;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import dev.knative.eventing.kafka.broker.receiver.IngressRequestHandler;
import dev.knative.eventing.kafka.broker.receiver.RequestContext;
import dev.knative.eventing.kafka.broker.receiver.RequestToRecordMapper;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;

/**
 * Implementation of {@link IngressRequestHandler} that will produce incoming requests using the provided producer.
 * <p>
 * Instances of this class can be shared among verticles, given the {@code requestToRecordMapper} provided is shareable among verticles.
 */
public class IngressRequestHandlerImpl implements IngressRequestHandler {

  static final Tag UNKNOWN_EVENT_TYPE_TAG = Tag.of(Metrics.Tags.EVENT_TYPE, "unknown");

  static final int MAPPER_FAILED = BAD_REQUEST.code();
  static final Tags MAPPER_FAILED_COMMON_TAGS = Tags.of(
    Tag.of(Metrics.Tags.RESPONSE_CODE_CLASS, "4xx"),
    Tag.of(Metrics.Tags.RESPONSE_CODE, Integer.toString(MAPPER_FAILED)),
    UNKNOWN_EVENT_TYPE_TAG
  );

  static final int RECORD_PRODUCED = ACCEPTED.code();
  static final Tags RECORD_PRODUCED_COMMON_TAGS = Tags.of(
    Tag.of(Metrics.Tags.RESPONSE_CODE, Integer.toString(RECORD_PRODUCED)),
    Tag.of(Metrics.Tags.RESPONSE_CODE_CLASS, "2xx")
  );

  static final int FAILED_TO_PRODUCE = SERVICE_UNAVAILABLE.code();
  static final Tags FAILED_TO_PRODUCE_COMMON_TAGS = Tags.of(
    Tag.of(Metrics.Tags.RESPONSE_CODE, Integer.toString(FAILED_TO_PRODUCE)),
    Tag.of(Metrics.Tags.RESPONSE_CODE_CLASS, "5xx")
  );

  private static final Logger logger = LoggerFactory.getLogger(IngressRequestHandlerImpl.class);

  private final RequestToRecordMapper requestToRecordMapper;
  private final MeterRegistry meterRegistry;

  public IngressRequestHandlerImpl(final RequestToRecordMapper requestToRecordMapper,
                                   final MeterRegistry meterRegistry) {
    this.requestToRecordMapper = requestToRecordMapper;
    this.meterRegistry = meterRegistry;
  }

  @Override
  public void handle(final RequestContext requestContext, final IngressProducer producer) {

    final var resourceTags = Tags.of(
      Tag.of("name", producer.getReference().getName()),
      Tag.of("namespace_name", producer.getReference().getNamespace())
    );

    requestToRecordMapper
      .requestToRecord(requestContext.getRequest(), producer.getTopic())
      .onFailure(cause -> {
        // Conversion to record failed
        requestContext.getRequest().response().setStatusCode(MAPPER_FAILED).end();

        final var tags = MAPPER_FAILED_COMMON_TAGS.and(resourceTags);
        Metrics.eventDispatchLatency(tags).register(meterRegistry).record(requestContext.performLatency());
        Metrics.eventCount(tags).register(meterRegistry).increment();

        logger.warn("Failed to convert request to record {}",
          keyValue("path", requestContext.getRequest().path()),
          cause
        );
      })
      .compose(record -> {
        // Conversion to record succeeded, let's push it to Kafka
        if (logger.isDebugEnabled()) {
          final var span = Span.fromContextOrNull(Context.current());
          if (span != null) {
            logger.debug("Received event {} {}",
              keyValue("event", record.value()),
              keyValue(TracingConfig.TRACE_ID_KEY, span.getSpanContext().getTraceId())
            );
          } else {
            logger.debug("Received event {}", keyValue("event", record.value()));
          }
        }

        // Decorate the span with event specific attributed
        TracingSpan.decorateCurrentWithEvent(record.value());

        final var eventTypeTag = Tag.of(Metrics.Tags.EVENT_TYPE, record.value().getType());

        return publishRecord(producer, record)
          .onSuccess(m -> {
            requestContext.getRequest().response().setStatusCode(RECORD_PRODUCED).end();

            final var tags = RECORD_PRODUCED_COMMON_TAGS
              .and(resourceTags)
              .and(eventTypeTag);
            Metrics.eventDispatchLatency(tags).register(meterRegistry).record(requestContext.performLatency());
            Metrics.eventCount(tags).register(meterRegistry).increment();

          })
          .onFailure(cause -> {
            requestContext.getRequest().response().setStatusCode(FAILED_TO_PRODUCE).end();

            final var tags = FAILED_TO_PRODUCE_COMMON_TAGS
              .and(resourceTags)
              .and(eventTypeTag);
            Metrics.eventDispatchLatency(tags).register(meterRegistry).record(requestContext.performLatency());
            Metrics.eventCount(tags).register(meterRegistry).increment();

            logger.warn("Failed to produce record {}",
              keyValue("path", requestContext.getRequest().path()),
              cause
            );
          });
      });
  }

  private static Future<RecordMetadata> publishRecord(final IngressProducer ingress,
                                                      final KafkaProducerRecord<String, CloudEvent> record) {
    return ingress.send(record)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          if (logger.isDebugEnabled()) {
            logger.debug("Record produced {} {} {} {} {}",
              keyValue("topic", record.topic()),
              keyValue("partition", ar.result().getPartition()),
              keyValue("offset", ar.result().getOffset()),
              keyValue("value", record.value()),
              keyValue("headers", record.headers())
            );
          }
        } else {
          logger.error("Failed to send record {} {}",
            keyValue("topic", record.topic()),
            ar.cause()
          );
        }
      });
  }

}
