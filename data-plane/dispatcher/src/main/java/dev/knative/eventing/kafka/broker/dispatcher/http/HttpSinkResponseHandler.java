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

import dev.knative.eventing.kafka.broker.dispatcher.SinkResponseHandler;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.util.Objects;

public final class HttpSinkResponseHandler implements SinkResponseHandler<HttpClientResponse> {

  private final String topic;
  private final KafkaProducer<String, CloudEvent> producer;

  /**
   * All args constructor.
   *
   * @param topic    topic to produce records.
   * @param producer Kafka producer.
   */
  public HttpSinkResponseHandler(
      final String topic,
      final KafkaProducer<String, CloudEvent> producer) {

    Objects.requireNonNull(topic, "provide topic");
    Objects.requireNonNull(producer, "provide producer");

    this.topic = topic;
    this.producer = producer;
  }

  /**
   * Handle the given response.
   *
   * @param response response to handle
   * @return a succeeded or failed future.
   */
  @Override
  public Future<Object> handle(final HttpClientResponse response) {
    return VertxMessageFactory.createReader(response)
        // TODO is this conversion really necessary?
        //      can be used Message?
        .map(MessageReader::toEvent)
        .compose(this::record)
        .compose(this::send, Future::succeededFuture); // always succeeded -> no event in response
  }

  private Future<KafkaProducerRecord<String, CloudEvent>> record(final CloudEvent event) {
    if (event == null) {
      return Future.failedFuture(new IllegalArgumentException("event cannot be null"));
    }
    // TODO set the correct producer record key
    return Future.succeededFuture(KafkaProducerRecord.create(topic, "", event));
  }

  private Future<Object> send(final KafkaProducerRecord<String, CloudEvent> record) {
    final Promise<RecordMetadata> promise = Promise.promise();
    producer.send(record, promise);
    return promise.future()
        .compose(Future::succeededFuture, Future::succeededFuture);
  }
}
