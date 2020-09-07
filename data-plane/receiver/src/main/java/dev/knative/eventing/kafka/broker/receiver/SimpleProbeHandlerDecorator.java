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

package dev.knative.eventing.kafka.broker.receiver;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

/**
 * SimpleProbeHandlerDecorator is a handler decorator that respond to specified liveness and
 * readiness probes.
 */
public class SimpleProbeHandlerDecorator implements Handler<HttpServerRequest> {

  protected static final int STATUS_OK = HttpResponseStatus.OK.code();

  private final String livenessPath;
  private final String readinessPath;
  private final Handler<HttpServerRequest> handler;

  /**
   * All args constructor for creating a new SimpleProbeHandlerDecorator.
   *
   * @param livenessPath  request path at which respond to liveness checks.
   * @param readinessPath request path at which respond to readiness checks.
   * @param handler       next handler
   */
  public SimpleProbeHandlerDecorator(
    final String livenessPath,
    final String readinessPath,
    final Handler<HttpServerRequest> handler) {
    this.livenessPath = livenessPath;
    this.readinessPath = readinessPath;
    this.handler = handler;
  }

  @Override
  public void handle(final HttpServerRequest request) {
    if (isProbeRequest(request)) {
      request.response().setStatusCode(STATUS_OK).end();
      return;
    }

    this.handler.handle(request);
  }

  private boolean isProbeRequest(final HttpServerRequest request) {
    return request.method().equals(HttpMethod.GET)
      && (request.path().equals(livenessPath) || request.path().equals(readinessPath));
  }
}
