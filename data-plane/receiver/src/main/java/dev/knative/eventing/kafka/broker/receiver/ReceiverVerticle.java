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

import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import java.util.Objects;

public class ReceiverVerticle extends AbstractVerticle {

  private final HttpServerOptions httpServerOptions;
  private final Handler<HttpServerRequest> requestHandler;
  private final ResourcesReconciler resourcesReconciler;

  private HttpServer server;
  private MessageConsumer<Object> messageConsumer;

  /**
   * Create a new HttpVerticle.
   *
   * @param httpServerOptions server options.
   * @param requestHandler    request handler.
   */
  public ReceiverVerticle(
    final HttpServerOptions httpServerOptions,
    final Handler<HttpServerRequest> requestHandler,
    final ResourcesReconciler resourcesReconciler) {
    Objects.requireNonNull(httpServerOptions, "provide http server options");
    Objects.requireNonNull(requestHandler, "provide request handler");
    Objects.requireNonNull(resourcesReconciler, "provide resources reconciler");

    this.httpServerOptions = httpServerOptions;
    this.requestHandler = requestHandler;
    this.resourcesReconciler = resourcesReconciler;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    this.messageConsumer = ResourcesReconcilerMessageHandler.start(vertx.eventBus(), this.resourcesReconciler);
    this.server = vertx.createHttpServer(httpServerOptions);

    this.server.requestHandler(requestHandler)
      .listen(httpServerOptions.getPort(), httpServerOptions.getHost())
      .<Void>mapEmpty()
      .onComplete(startPromise);
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    CompositeFuture.all(
      server.close().mapEmpty(),
      messageConsumer.unregister()
    )
      .<Void>mapEmpty()
      .onComplete(stopPromise);
  }
}
