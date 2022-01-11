package dev.knative.eventing.kafka.broker.dispatcher.impl;

import java.net.http.HttpResponse;

public class ResponseFailureException extends RuntimeException {

  private final HttpResponse<?> response;

  public ResponseFailureException(final HttpResponse response, final String msg) {
    super(msg);
    this.response = response;
  }

  public ResponseFailureException(final HttpResponse<?> response,
                                  final Throwable ex) {
    super(ex);
    this.response = response;
  }

  public HttpResponse<?> getResponse() {
    return response;
  }
}
