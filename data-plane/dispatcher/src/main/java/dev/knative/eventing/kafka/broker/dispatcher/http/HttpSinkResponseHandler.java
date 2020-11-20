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

package dev.knative.eventing.kafka.broker.dispatcher.http;

import dev.knative.eventing.kafka.broker.core.cloudevents.PartitionKey;
import dev.knative.eventing.kafka.broker.core.tracing.TracingSpan;
import dev.knative.eventing.kafka.broker.dispatcher.SinkResponseHandler;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.rw.CloudEventRWException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.Objects;

public final class HttpSinkResponseHandler implements SinkResponseHandler<HttpResponse<Buffer>> {

  private final String topic;
  private final KafkaProducer<String, CloudEvent> producer;
  private final Vertx vertx;

  /**
   * All args constructor.
   *
   * @param vertx    vertx instance
   * @param topic    topic to produce records.
   * @param producer Kafka producer.
   */
  public HttpSinkResponseHandler(
    final Vertx vertx,
    final String topic,
    final KafkaProducer<String, CloudEvent> producer) {

    Objects.requireNonNull(vertx, "provide vertx");
    Objects.requireNonNull(topic, "provide topic");
    Objects.requireNonNull(producer, "provide producer");

    this.topic = topic;
    this.producer = producer;
    this.vertx = vertx;
  }

  /**
   * Handle the given response.
   *
   * @param response response to handle
   * @return a succeeded or failed future.
   */
  @Override
  public Future<Void> handle(final HttpResponse<Buffer> response) {
    // TODO if it's in structured, the SDK just calls getBytes on the response.body() which might lead to NPE.
    MessageReader messageReader = VertxMessageFactory.createReader(response);
    if (messageReader.getEncoding() == Encoding.UNKNOWN) {

      // When the sink returns a malformed event we return a failed future to avoid committing the message to Kafka.
      if (response.body() != null && response.body().length() > 0) {
        return Future.failedFuture(
          new IllegalResponseException("Unable to decode response: unknown encoding and non empty response")
        );
      }

      // Response is non-event, discard it
      return Future.succeededFuture();
    }

    CloudEvent event;
    try {
      event = messageReader.toEvent();
    } catch (CloudEventRWException e) {
      return Future.failedFuture(e);
    }
    if (event == null) {
      return Future.failedFuture(new IllegalArgumentException("event cannot be null"));
    }

    TracingSpan.decorateCurrent(vertx, event);

    return producer
      .send(KafkaProducerRecord.create(topic, PartitionKey.extract(event), event))
      .mapEmpty();
  }
}
