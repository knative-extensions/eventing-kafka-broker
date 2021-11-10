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
import dev.knative.eventing.kafka.broker.receiver.impl.handler.MethodNotAllowedHandler;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.ProbeHandler;
import dev.knative.eventing.kafka.broker.receiver.main.ReceiverEnv;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;
import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.PROBE_HASH_HEADER_NAME;
import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.isControlPlaneProbeRequest;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * This verticle is responsible for implementing the logic of the receiver.
 * <p>
 * The receiver is the component responsible for mapping incoming {@link io.cloudevents.CloudEvent} requests to specific Kafka topics.
 * In order to do so, this component:
 * <ul>
 *   <li>Starts an {@link HttpServer} listening for incoming events</li>
 *   <li>Starts a {@link ResourcesReconciler}, listen on the event bus for reconciliation events and keeps track of the {@link dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Ingress} objects and their {@code path => (topic, producer)} mapping</li>
 *   <li>Implements a request handler that invokes a series of {@code preHandlers} (which are assumed to complete synchronously) and then a final {@link IngressRequestHandler} to publish the record to Kafka</li>
 * </ul>
 */
public class ReceiverVerticle extends AbstractVerticle implements Handler<HttpServerRequest> {

  private static final Logger logger = LoggerFactory.getLogger(ReceiverVerticle.class);

  private final HttpServerOptions httpServerOptions;
  private final Function<Vertx, IngressProducerReconcilableStore> ingressProducerStoreFactory;
  private final IngressRequestHandler ingressRequestHandler;
  private final ReceiverEnv env;

  private HttpServer server;
  private MessageConsumer<Object> messageConsumer;
  private IngressProducerReconcilableStore ingressProducerStore;

  public ReceiverVerticle(final ReceiverEnv env,
                          final HttpServerOptions httpServerOptions,
                          final Function<Vertx, IngressProducerReconcilableStore> ingressProducerStoreFactory,
                          final IngressRequestHandler ingressRequestHandler) {
    Objects.requireNonNull(env);
    Objects.requireNonNull(ingressProducerStoreFactory);
    Objects.requireNonNull(ingressRequestHandler);

    this.env = env;
    this.httpServerOptions = httpServerOptions != null ? httpServerOptions : new HttpServerOptions();
    this.ingressProducerStoreFactory = ingressProducerStoreFactory;
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

    final var handler = new ProbeHandler(
      env.getLivenessProbePath(),
      env.getReadinessProbePath(),
      new MethodNotAllowedHandler(this)
    );

    this.server.requestHandler(handler)
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

  @Override
  public void handle(HttpServerRequest request) {

    // Look up for the ingress producer
    IngressProducer producer = this.ingressProducerStore.resolve(request.path());
    if (producer == null) {
      request.response().setStatusCode(NOT_FOUND.code()).end();
      logger.warn("Resource not found {}", keyValue("path", request.path()));
      return;
    }

    if (isControlPlaneProbeRequest(request)) {
      request.response()
        .putHeader(PROBE_HASH_HEADER_NAME, request.getHeader(PROBE_HASH_HEADER_NAME))
        .setStatusCode(OK.code()).end();
      return;
    }

    // Invoke the ingress request handler
    this.ingressRequestHandler.handle(request, producer);
  }
}
