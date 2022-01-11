package dev.knative.eventing.kafka.broker.dispatcher.impl;

import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import org.slf4j.LoggerFactory;

/**
 * This class implements a {@link ResponseHandler} that will convert the sink response into a {@link CloudEvent} and post it to a URL.
 */
public class ResponseToHttpEndpointHandler extends BaseResponseHandler implements ResponseHandler {

  private final CloudEventSender cloudEventSender;

  public ResponseToHttpEndpointHandler(CloudEventSender cloudEventSender) {
    super(LoggerFactory.getLogger(ResponseToKafkaTopicHandler.class));
    this.cloudEventSender = cloudEventSender;
  }

  @Override
  protected Future<Void> doHandleEvent(CloudEvent event) {
    return cloudEventSender.send(event).mapEmpty();
  }

  @Override
  public Future<Void> close() {
    return this.cloudEventSender.close().mapEmpty();
  }
}
