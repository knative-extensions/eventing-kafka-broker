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
package dev.knative.eventing.kafka.broker.receiver.impl;

import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import dev.knative.eventing.kafka.broker.receiver.IngressRequestHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

public class ReceiverVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(ReceiverVerticle.class);

  private final HttpServerOptions httpServerOptions;
  private final Function<Vertx, IngressProducerReconcilableStore> ingressProducerStoreFactory;
  private final Iterable<Handler<HttpServerRequest>> preHandlers;
  private final IngressRequestHandler ingressRequestHandler;

  private HttpServer server;
  private MessageConsumer<Object> messageConsumer;
  private IngressProducerReconcilableStore ingressProducerStore;

  public ReceiverVerticle(HttpServerOptions httpServerOptions,
                          Function<Vertx, IngressProducerReconcilableStore> ingressProducerStoreFactory,
                          Iterable<Handler<HttpServerRequest>> preHandlers,
                          IngressRequestHandler ingressRequestHandler) {
    Objects.requireNonNull(ingressProducerStoreFactory);
    Objects.requireNonNull(ingressRequestHandler);

    this.httpServerOptions = httpServerOptions != null ? httpServerOptions : new HttpServerOptions();
    this.ingressProducerStoreFactory = ingressProducerStoreFactory;
    this.preHandlers = preHandlers != null ? preHandlers : Collections.emptyList();
    this.ingressRequestHandler = ingressRequestHandler;
  }


  @Override
  public void start(final Promise<Void> startPromise) {
    this.ingressProducerStore = this.ingressProducerStoreFactory.apply(vertx);
    this.messageConsumer = ResourcesReconciler
      .builder()
      .watchIngress(this.ingressProducerStore)
      .buildAndListen(vertx);

    this.server = vertx.createHttpServer(httpServerOptions);

    this.server.requestHandler(this::handle)
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

  private void handle(HttpServerRequest request) {
    // Run preHandlers
    for (Handler<HttpServerRequest> preHandler : this.preHandlers) {
      preHandler.handle(request);
      // preHandler might end the request prematurely
      if (request.isEnded()) {
        return;
      }
    }

    // Look up for the ingress producer
    IngressProducer producer = this.ingressProducerStore.resolve(request.path());
    if (producer == null) {
      request.response().setStatusCode(NOT_FOUND.code()).end();

      logger.warn("Resource not found {}",
        keyValue("path", request.path())
      );
      return;
    }

    // Invoke the ingress request handler
    this.ingressRequestHandler.handle(request, producer);
  }
}
