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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.tracing.TracingSpan;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.impl.ResponseFailureException;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleFactoryImpl;
import io.cloudevents.CloudEvent;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.rw.CloudEventRWException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public final class WebClientCloudEventSender implements CloudEventSender {

  private static final Logger logger = LoggerFactory.getLogger(WebClientCloudEventSender.class);

  private static final long DEFAULT_TIMEOUT_MS = 600_000L;

  private final WebClient client;
  private final String target;

  private final Promise<Void> closePromise = Promise.promise();
  private final Function<Integer, Long> retryPolicyFunc;
  private final DataPlaneContract.EgressConfig egress;
  private final Vertx vertx;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicInteger inFlightRequests = new AtomicInteger(0);

  /**
   * All args constructor.
   *
   * @param vertx  Vertx context instance
   * @param client http client.
   * @param target subscriber URI
   * @param egress egress configuration
   */
  public WebClientCloudEventSender(final Vertx vertx,
                                   final WebClient client,
                                   final String target,
                                   final DataPlaneContract.EgressConfig egress) {
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(client, "provide client");
    if (target == null || target.equals("")) {
      throw new IllegalArgumentException("provide a target");
    }
    if (!URI.create(target).isAbsolute()) {
      throw new IllegalArgumentException("provide a valid target, provided target: " + target);
    }

    this.vertx = vertx;
    this.client = client;
    this.target = target;
    this.egress = egress;
    this.retryPolicyFunc = computeRetryPolicy(egress);
  }

  public Future<HttpResponse<Buffer>> send(final CloudEvent event) {
    return send(event, 0);
  }

  private Future<HttpResponse<Buffer>> send(final CloudEvent event, final int retryCounter) {
    logger.debug("Sending event {} {} {}",
      keyValue("id", event.getId()),
      keyValue("subscriberURI", target),
      keyValue("retry", retryCounter)
    );


    final Promise<HttpResponse<Buffer>> promise = Promise.promise();

    if (closed.get()) {
      // Once sender is closed, return a successful future to avoid retrying.
      promise.tryComplete(null);
    } else {
      try {
        TracingSpan.decorateCurrentWithEvent(event);
        requestEmitted();
        send(event, promise).
          onComplete(v -> requestCompleted());
      } catch (CloudEventRWException e) {
        logger.error("failed to write event to the request {}", keyValue("subscriberURI", target), e);
        promise.tryFail(e);
      }
    }

    // Handle closed return value.
    return promise.future().compose(
      r -> {
        if (r == null) {
          return Future.failedFuture("Sender closed for target=" + target);
        }
        return Future.succeededFuture(r);
      },
      // Retry and failures
      cause -> {
        if (retryCounter < egress.getRetry()) {
          Promise<HttpResponse<Buffer>> r = Promise.promise();
          final var delay = retryPolicyFunc.apply(retryCounter + 1);
          vertx.setTimer(delay, v -> send(event, retryCounter + 1).onComplete(r));
          return r.future();
        }

        return Future.failedFuture(cause);
      }
    );
  }

  private void requestCompleted() {
    inFlightRequests.decrementAndGet();
    if (closed.get() && inFlightRequests.get() == 0) {
      closePromise.tryComplete(null);
    }
  }

  private void requestEmitted() {
    inFlightRequests.incrementAndGet();
  }

  private Future<?> send(final CloudEvent event, final Promise<HttpResponse<Buffer>> breaker) {
    return VertxMessageFactory
      .createWriter(client.postAbs(target)
        .timeout(this.egress.getTimeout() <= 0 ? DEFAULT_TIMEOUT_MS : this.egress.getTimeout())
        .putHeader("Prefer", "reply"))
      .writeBinary(event)
      .onFailure(ex -> {
        logError(event, ex);
        breaker.tryFail(ex);
      })
      .onSuccess(response -> {

        if (isRetryableStatusCode(response.statusCode())) {
          logError(event, response);
          breaker.tryFail(new ResponseFailureException(
            response,
            "Received failure response, status code: " + response.statusCode())
          );
          return;
        }

        if (response.statusCode() >= 300) {
          logError("Received a non-retryable status code that is not 2xx.", event, response);
        }

        breaker.tryComplete(response);
      });
  }

  private void logError(final String prefix, final CloudEvent event, final HttpResponse<Buffer> response) {
    if (logger.isDebugEnabled()) {
      logger.error(prefix + "failed to send event to subscriber {} {} {}",
        keyValue("target", target),
        keyValue("statusCode", response.statusCode()),
        keyValue("event", event)
      );
    } else {
      logger.error(prefix + " failed to send event to subscriber {} {}",
        keyValue("target", target),
        keyValue("statusCode", response.statusCode())
      );
    }
  }

  private void logError(final CloudEvent event, final HttpResponse<Buffer> response) {
    logError("", event, response);
  }

  private void logError(final CloudEvent event, Throwable ex) {
    if (logger.isDebugEnabled()) {
      logger.error("failed to send event to subscriber {} {}", keyValue("target", target), keyValue("event", event), ex);
    } else {
      logger.error("failed to send event to subscriber {}", keyValue("target", target), ex);
    }
  }

  @Override
  public Future<Void> close() {
    this.closed.set(true);

    logger.info("Close {} {}",
      keyValue("target", target),
      keyValue("inFlightRequests", inFlightRequests)
    );

    if (inFlightRequests.get() == 0) {
      closePromise.tryComplete(null);
    }

    final Function<Void, Future<Void>> closeF = v -> {
      this.client.close();
      return Future.succeededFuture();
    };

    return closePromise.future()
      .compose(
        v -> closeF.apply(null),
        v -> closeF.apply(null)
      );
  }

  static boolean isRetryableStatusCode(final int statusCode) {
    // From https://github.com/knative/specs/blob/c348f501de9eb998b4fd010c54d9127033ee41be/specs/eventing/data-plane.md#event-acknowledgement-and-delivery-retry
    return statusCode >= 500 || // Generic error
      statusCode == 404 || // Endpoint does not exist
      statusCode == 408 || // Request Timeout
      statusCode == 409 || // Conflict / Processing in progress
      statusCode == 429;   // Too Many Requests / Overloaded
  }

  public static Function<Integer, Long> computeRetryPolicy(final DataPlaneContract.EgressConfig egress) {
    if (egress != null && egress.getBackoffDelay() > 0) {
      final var delay = egress.getBackoffDelay();
      return switch (egress.getBackoffPolicy()) {
        case Linear -> retryCount -> delay * retryCount;
        case Exponential, UNRECOGNIZED -> retryCount -> delay * Math.round(Math.pow(2, retryCount));
      };
    }
    return retry -> 0L; // Default Vert.x retry policy, it means don't retry
  }

}
