package dev.knative.eventing.kafka.broker.dispatcher;

import io.vertx.core.Future;

@FunctionalInterface
public interface SinkResponseHandler<R> {

  Future<Object> handle(final R response);
}
