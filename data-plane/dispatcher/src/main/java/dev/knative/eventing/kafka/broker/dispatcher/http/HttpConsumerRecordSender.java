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

import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import io.cloudevents.CloudEvent;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.net.URI;
import java.util.Objects;

public final class HttpConsumerRecordSender implements
    ConsumerRecordSender<String, CloudEvent, HttpClientResponse> {

  private final HttpClient client;
  private final String subscriberURI;

  /**
   * All args constructor.
   *
   * @param client        http client.
   * @param subscriberURI subscriber URI
   */
  public HttpConsumerRecordSender(
      final HttpClient client,
      final String subscriberURI) {

    Objects.requireNonNull(client, "provide client");
    Objects.requireNonNull(subscriberURI, "provide subscriber URI");
    if (subscriberURI.equals("") || !URI.create(subscriberURI).isAbsolute()) {
      throw new IllegalArgumentException("provide a valid subscriber URI");
    }

    this.client = client;
    this.subscriberURI = subscriberURI;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Future<HttpClientResponse> send(final KafkaConsumerRecord<String, CloudEvent> record) {
    final Promise<HttpClientResponse> promise = Promise.promise();
    final var request = client.postAbs(subscriberURI)
        .exceptionHandler(promise::tryFail)
        .handler(response -> {
          if (response.statusCode() >= 300) {
            // TODO determine which status codes are retryable
            //  (channels -> https://github.com/knative/eventing/issues/2411).
            promise.tryFail("response status code is not 2xx - got: " + response.statusCode());

            return;
          }

          promise.tryComplete(response);
        });

    VertxMessageFactory.createWriter(request).writeBinary(record.value());

    return promise.future();
  }
}
