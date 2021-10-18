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
package dev.knative.eventing.kafka.broker.dispatcher.impl.http;

import dev.knative.eventing.kafka.broker.core.tracing.TracingSpan;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import io.cloudevents.CloudEvent;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.rw.CloudEventRWException;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.net.URI;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public final class WebClientCloudEventSender implements CloudEventSender {

  private static final Logger logger = LoggerFactory.getLogger(WebClientCloudEventSender.class);

  private final WebClient client;
  private final CircuitBreaker circuitBreaker;
  private final String target;

  /**
   * All args constructor.
   *
   * @param client         http client.
   * @param circuitBreaker circuit breaker
   * @param target         subscriber URI
   */
  public WebClientCloudEventSender(final WebClient client, final CircuitBreaker circuitBreaker, final String target) {
    Objects.requireNonNull(client, "provide client");
    if (target == null || target.equals("") || !URI.create(target).isAbsolute()) {
      throw new IllegalArgumentException("provide a valid target");
    }
    Objects.requireNonNull(circuitBreaker, "provide circuitBreaker");

    this.client = client;
    this.target = target;
    this.circuitBreaker = circuitBreaker;
  }

  @Override
  public Future<HttpResponse<Buffer>> send(CloudEvent event) {
    TracingSpan.decorateCurrentWithEvent(event);

    return circuitBreaker.execute(breaker -> {
      try {
        send(event, breaker);
      } catch (CloudEventRWException e) {
        logger.error("failed to write event to the request {}", keyValue("subscriberURI", target), e);
        breaker.tryFail(e);
      }
    });
  }

  private void send(final CloudEvent event, final Promise<HttpResponse<Buffer>> breaker) {
    VertxMessageFactory
      .createWriter(client.postAbs(target))
      .writeBinary(event)
      .onFailure(breaker::tryFail)
      .onSuccess(response -> {

        if (response.statusCode() >= 300 || response.statusCode() < 200) {
          logError(event, response);
          // TODO determine which status codes are retryable
          //  (channels -> https://github.com/knative/eventing/issues/2411)
          breaker.tryFail("response status code is not 2xx - got: " + response.statusCode());
          return;
        }

        breaker.tryComplete(response);
      });
  }

  private void logError(final CloudEvent event, final HttpResponse<Buffer> response) {
    if (logger.isDebugEnabled()) {
      logger.error("failed to send event to subscriber {} {} {}",
        keyValue("target", target),
        keyValue("statusCode", response.statusCode()),
        keyValue("event", event)
      );
    } else {
      logger.error("failed to send event to subscriber {} {}",
        keyValue("target", target),
        keyValue("statusCode", response.statusCode())
      );
    }
  }

  @Override
  public Future<Void> close() {
    this.circuitBreaker.close();
    this.client.close();
    return Future.succeededFuture();
  }
}
