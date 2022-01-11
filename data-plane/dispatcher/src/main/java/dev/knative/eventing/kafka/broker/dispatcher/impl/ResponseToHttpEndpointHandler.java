package dev.knative.eventing.kafka.broker.dispatcher.impl;

import dev.knative.eventing.kafka.broker.core.tracing.TracingSpan;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import io.cloudevents.CloudEvent;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

/**
 * This class implements a {@link ResponseHandler} that will convert the sink response into a {@link CloudEvent} and post it to a URL.
 */
// TODO: lots of dupe
// TODO: metrics?
public class ResponseToHttpEndpointHandler implements ResponseHandler {

  private static final Logger logger = LoggerFactory.getLogger(ResponseToKafkaTopicHandler.class);

  private final CloudEventSender cloudEventSender;

  public ResponseToHttpEndpointHandler(CloudEventSender cloudEventSender) {
    this.cloudEventSender = cloudEventSender;
  }

  @Override
  public Future<Void> handle(HttpResponse<Buffer> response) {
    CloudEvent event;

    try {
      event = VertxMessageFactory.createReader(response).toEvent();
    } catch (final Exception ex) {
      if (maybeIsNotEvent(response)) {
        if (logger.isDebugEnabled()) {
          logger.debug(
            "Response is not recognized as event, discarding it {} {} {}",
            keyValue("response", response),
            keyValue("response.body", response == null || response.body() == null ? "null" : response.body()),
            keyValue("response.body.len",
              response == null || response.body() == null ? "null" : response.body().length())
          );
        }
        return Future.succeededFuture();
      }

      // When the sink returns a malformed event we return a failed future to avoid committing the message to Kafka.
      return Future.failedFuture(
        new IllegalStateException("Unable to decode response: unknown encoding and non empty response", ex)
      );
    }

    if (event == null) {
      return Future.failedFuture(new IllegalArgumentException("event cannot be null"));
    }

    TracingSpan.decorateCurrentWithEvent(event);

    return cloudEventSender.send(event).mapEmpty();
  }

  private static boolean maybeIsNotEvent(final HttpResponse<Buffer> response) {
    // This checks whether there is something in the body or not, though binary events can contain only headers and they
    // are valid Cloud Events.
    return response == null || response.body() == null || response.body().length() <= 0;
  }

  @Override
  public Future<Void> close() {
    return this.cloudEventSender.close().mapEmpty();
  }
}
