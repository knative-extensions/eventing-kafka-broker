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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import java.util.Objects;

public class HttpVerticle extends AbstractVerticle {

  private final HttpServerOptions httpServerOptions;
  private final Handler<HttpServerRequest> requestHandler;
  private HttpServer server;

  /**
   * Create a new HttpVerticle.
   *
   * @param httpServerOptions server options.
   * @param requestHandler    request handler.
   */
  public HttpVerticle(
    final HttpServerOptions httpServerOptions,
    final Handler<HttpServerRequest> requestHandler) {

    Objects.requireNonNull(httpServerOptions, "provide http server options");
    Objects.requireNonNull(requestHandler, "provide request handler");

    this.httpServerOptions = httpServerOptions;
    this.requestHandler = requestHandler;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    server = vertx.createHttpServer(httpServerOptions);
    server.requestHandler(requestHandler)
      .listen(httpServerOptions.getPort(), httpServerOptions.getHost())
      .<Void>mapEmpty()
      .onComplete(startPromise);
  }

  @Override
  public void stop() {
    server.close();
  }
}
