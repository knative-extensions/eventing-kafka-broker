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
package dev.knative.eventing.kafka.broker.receiver.impl;

import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig;
import dev.knative.eventing.kafka.broker.core.tracing.TracingSpan;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import dev.knative.eventing.kafka.broker.receiver.IngressRequestHandler;
import dev.knative.eventing.kafka.broker.receiver.RequestToRecordMapper;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
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

  public static final int MAPPER_FAILED = BAD_REQUEST.code();
  public static final int FAILED_TO_PRODUCE = SERVICE_UNAVAILABLE.code();
  public static final int RECORD_PRODUCED = ACCEPTED.code();

  private static final Logger logger = LoggerFactory.getLogger(IngressRequestHandlerImpl.class);

  private final RequestToRecordMapper requestToRecordMapper;
  private final Counter badRequestCounter;
  private final Counter produceEventsCounter;

  public IngressRequestHandlerImpl(
    RequestToRecordMapper requestToRecordMapper,
    Counter badRequestCounter,
    Counter produceEventsCounter) {
    this.requestToRecordMapper = requestToRecordMapper;
    this.badRequestCounter = badRequestCounter;
    this.produceEventsCounter = produceEventsCounter;
  }

  @Override
  public void handle(HttpServerRequest request, IngressProducer producer) {
    requestToRecordMapper
      .requestToRecord(request, producer.getTopic())
      .onFailure(cause -> {
        // Conversion to record failed
        request.response().setStatusCode(MAPPER_FAILED).end();
        badRequestCounter.increment();

        logger.warn("Failed to convert request to record {}",
          keyValue("path", request.path()),
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

        // Publish the record
        return publishRecord(producer, record);
      }).onComplete(ar -> {
      // Write the response back
      if (ar.succeeded()) {
        request.response()
          .setStatusCode(RECORD_PRODUCED)
          .end();
      } else {
        request.response()
          .setStatusCode(FAILED_TO_PRODUCE)
          .end();
      }
    });
  }

  private Future<RecordMetadata> publishRecord(IngressProducer ingress,
                                               KafkaProducerRecord<String, CloudEvent> record) {
    return ingress.send(record)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          produceEventsCounter.increment();
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
