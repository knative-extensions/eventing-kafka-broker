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

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordSender;
import io.cloudevents.CloudEvent;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.rw.CloudEventRWException;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.net.URI;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpConsumerRecordSender implements
    ConsumerRecordSender<String, CloudEvent, HttpResponse<Buffer>> {

  private static final Logger logger = LoggerFactory.getLogger(HttpConsumerRecordSender.class);

  private final WebClient client;
  private final String subscriberURI;

  /**
   * All args constructor.
   *
   * @param client        http client.
   * @param subscriberURI subscriber URI
   */
  public HttpConsumerRecordSender(
      final WebClient client,
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
  public Future<HttpResponse<Buffer>> send(final KafkaConsumerRecord<String, CloudEvent> record) {
    try {
      return VertxMessageFactory
          .createWriter(client.postAbs(subscriberURI))
          .writeBinary(record.value())
          .compose(response -> {
            if (response.statusCode() >= 300 || response.statusCode() < 200) {
              if (logger.isDebugEnabled()) {
                logger.error("failed to send event to subscriber {} {} {}",
                    keyValue("subscriberURI", subscriberURI),
                    keyValue("statusCode", response.statusCode()),
                    keyValue("event", record.value())
                );
              } else {
                logger.error("failed to send event to subscriber {} {}",
                    keyValue("subscriberURI", subscriberURI),
                    keyValue("statusCode", response.statusCode())
                );
              }

              // TODO determine which status codes are retryable
              //  (channels -> https://github.com/knative/eventing/issues/2411)
              return Future
                  .failedFuture("response status code is not 2xx - got: " + response.statusCode());
            }

            return Future.succeededFuture(response);
          });
    } catch (CloudEventRWException e) {
      logger.error("failed to write event to the request {}",
          keyValue("subscriberURI", subscriberURI),
          e
      );
      return Future.failedFuture(e);
    }
  }
}
