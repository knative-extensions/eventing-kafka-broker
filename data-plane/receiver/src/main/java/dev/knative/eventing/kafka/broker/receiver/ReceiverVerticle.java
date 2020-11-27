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
package dev.knative.eventing.kafka.broker.receiver;

import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerImpl;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import java.util.Objects;
import java.util.function.Function;

public class ReceiverVerticle extends AbstractVerticle {

  private final HttpServerOptions httpServerOptions;

  private HttpServer server;
  private MessageConsumer<Object> messageConsumer;
  private final Function<Vertx, RequestMapper<String, CloudEvent>> requestHandlerFactory;
  private final Function<Handler<HttpServerRequest>, Handler<HttpServerRequest>>[] handlerDecoratorFactories;

  /**
   * Create a new HttpVerticle.
   *
   * @param httpServerOptions         server options.
   * @param requestHandlerFactory     request handler factory.
   * @param handlerDecoratorFactories request handler decorators functions
   */
  @SafeVarargs
  public ReceiverVerticle(
    final HttpServerOptions httpServerOptions,
    final Function<Vertx, RequestMapper<String, CloudEvent>> requestHandlerFactory,
    Function<Handler<HttpServerRequest>, Handler<HttpServerRequest>>... handlerDecoratorFactories) {
    Objects.requireNonNull(httpServerOptions, "provide http server options");
    Objects.requireNonNull(requestHandlerFactory, "provide request handler");

    this.httpServerOptions = httpServerOptions;
    this.requestHandlerFactory = requestHandlerFactory;
    this.handlerDecoratorFactories = handlerDecoratorFactories;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    final var requestMapper = this.requestHandlerFactory.apply(vertx);

    this.messageConsumer = ResourcesReconcilerMessageHandler.start(
      vertx.eventBus(),
      ResourcesReconcilerImpl
        .builder()
        .watchIngress(requestMapper)
        .build()
    );
    this.server = vertx.createHttpServer(httpServerOptions);

    Handler<HttpServerRequest> requestHandler = requestMapper;
    for (final var handlerDecoratorFactory : this.handlerDecoratorFactories) {
      requestHandler = handlerDecoratorFactory.apply(requestHandler);
    }

    this.server.requestHandler(requestHandler)
      .exceptionHandler(startPromise::tryFail)
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
