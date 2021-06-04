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
package dev.knative.eventing.kafka.broker.receiver;

import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig;
import dev.knative.eventing.kafka.broker.core.tracing.TracingSpan;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class CloudEventRequestToRecordMapper implements RequestToRecordMapper {

  private static final Logger logger = LoggerFactory.getLogger(CloudEventRequestToRecordMapper.class);

  public CloudEventRequestToRecordMapper() {
  }

  @Override
  public Future<KafkaProducerRecord<String, CloudEvent>> recordFromRequest(
    final HttpServerRequest request,
    final String topic) {

    return VertxMessageFactory.createReader(request)
      .map(MessageReader::toEvent)
      .map(event -> {
        if (event == null) {
          throw new IllegalArgumentException("event cannot be null");
        }

        if (logger.isDebugEnabled()) {
          final var span = Span.fromContextOrNull(Context.current());
          if (span != null) {
            logger.debug("Received event {} {}",
              keyValue("event", event),
              keyValue(TracingConfig.TRACE_ID_KEY, span.getSpanContext().getTraceId())
            );
          } else {
            logger.debug("Received event {}", keyValue("event", event));
          }
        }

        TracingSpan.decorateCurrentWithEvent(event);
        return KafkaProducerRecord.create(topic, event);
      });
  }
}
