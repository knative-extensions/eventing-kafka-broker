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

import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import dev.knative.eventing.kafka.broker.receiver.IngressRequestHandler;
import dev.knative.eventing.kafka.broker.receiver.RequestContext;
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
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;
import java.io.File;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;
import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.PROBE_HASH_HEADER_NAME;
import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.isControlPlaneProbeRequest;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * This verticle is responsible for implementing the logic of the receiver.
 * <p>
 * The receiver is the component responsible for mapping incoming
 * {@link io.cloudevents.CloudEvent} requests to specific Kafka topics.
 * In order to do so, this component:
 * <ul>
 * <li>Starts two {@link HttpServer}, one with http, and one with https,
 * listening for incoming events</li>
 * <li>Starts a {@link ResourcesReconciler}, listen on the event bus for
 * reconciliation events and keeps track of the
 * {@link dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Ingress}
 * objects and their {@code path => (topic, producer)} mapping</li>
 * <li>Implements a request handler that invokes a series of {@code preHandlers}
 * (which are assumed to complete synchronously) and then a final
 * {@link IngressRequestHandler} to publish the record to Kafka</li>
 * </ul>
 */
public class ReceiverVerticle extends AbstractVerticle implements Handler<HttpServerRequest> {

  private static final Logger logger = LoggerFactory.getLogger(ReceiverVerticle.class);
  private static final String SECRET_VOLUME_PATH = "/etc/receiver-secret-volume";
  private static final String TLS_KEY_FILE_PATH = SECRET_VOLUME_PATH + "/tls.key";
  private static final String TLS_CRT_FILE_PATH = SECRET_VOLUME_PATH + "/tls.crt";

  private final HttpServerOptions httpServerOptions;
  private final HttpServerOptions httpsServerOptions;
  private final Function<Vertx, IngressProducerReconcilableStore> ingressProducerStoreFactory;
  private final IngressRequestHandler ingressRequestHandler;
  private final ReceiverEnv env;

  private HttpServer httpServer;
  private HttpServer httpsServer;
  private MessageConsumer<Object> messageConsumer;
  private IngressProducerReconcilableStore ingressProducerStore;

  public ReceiverVerticle(final ReceiverEnv env,
      final HttpServerOptions httpServerOptions,
      final HttpServerOptions httpsServerOptions,
      final Function<Vertx, IngressProducerReconcilableStore> ingressProducerStoreFactory,
      final IngressRequestHandler ingressRequestHandler) {
    Objects.requireNonNull(env);
    Objects.requireNonNull(httpServerOptions);
    Objects.requireNonNull(httpsServerOptions);
    Objects.requireNonNull(ingressProducerStoreFactory);
    Objects.requireNonNull(ingressRequestHandler);

    this.env = env;
    this.httpServerOptions = httpServerOptions != null ? httpServerOptions : new HttpServerOptions();
    this.httpsServerOptions = httpsServerOptions;
    this.ingressProducerStoreFactory = ingressProducerStoreFactory;
    this.ingressRequestHandler = ingressRequestHandler;
    
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    this.ingressProducerStore = this.ingressProducerStoreFactory.apply(vertx);
    this.messageConsumer = ResourcesReconciler
        .builder()
        .watchIngress(IngressReconcilerListener.all(this.ingressProducerStore, this.ingressRequestHandler))
        .buildAndListen(vertx);

    this.httpServer = vertx.createHttpServer(this.httpServerOptions);

    // check whether the secret volume is mounted
    File secretVolume = new File(SECRET_VOLUME_PATH);
    if (secretVolume.exists()) {
      // The secret volume is mounted, we should start the https server
      // check whether the tls.key and tls.crt files exist
      File tlsKeyFile = new File(TLS_KEY_FILE_PATH);
      File tlsCrtFile = new File(TLS_CRT_FILE_PATH);

      if (tlsKeyFile.exists() && tlsCrtFile.exists() && httpsServerOptions != null) {
        PemKeyCertOptions keyCertOptions = new PemKeyCertOptions()
            .setKeyPath(TLS_KEY_FILE_PATH)
            .setCertPath(TLS_CRT_FILE_PATH);
        this.httpsServerOptions
            .setSsl(true)
            .setPemKeyCertOptions(keyCertOptions);

        this.httpsServer = vertx.createHttpServer(this.httpsServerOptions);

      }

    } 

    final var handler = new ProbeHandler(
        env.getLivenessProbePath(),
        env.getReadinessProbePath(),
        new MethodNotAllowedHandler(this));

    if (this.httpsServer != null) {
      CompositeFuture.all(
          this.httpServer.requestHandler(handler)
              .exceptionHandler(startPromise::tryFail)
              .listen(this.httpServerOptions.getPort(), this.httpServerOptions.getHost()),

          this.httpsServer.requestHandler(handler)
              .exceptionHandler(startPromise::tryFail)
              .listen(this.httpsServerOptions.getPort(), this.httpsServerOptions.getHost()))
          .<Void>mapEmpty().onComplete(startPromise);
    } else {
      this.httpServer.requestHandler(handler)
          .exceptionHandler(startPromise::tryFail)
          .listen(this.httpServerOptions.getPort(), this.httpServerOptions.getHost())
          .<Void>mapEmpty().onComplete(startPromise);
    }
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    CompositeFuture.all(
        (this.httpServer != null ? this.httpServer.close().mapEmpty() : Future.succeededFuture()),
        (this.httpsServer != null ? this.httpsServer.close().mapEmpty() : Future.succeededFuture()),
        (this.messageConsumer != null ? this.messageConsumer.unregister() : Future.succeededFuture())).<Void>mapEmpty()
        .onComplete(stopPromise);
  }

  @Override
  public void handle(HttpServerRequest request) {

    final var requestContext = new RequestContext(request);

    // Look up for the ingress producer
    IngressProducer producer = this.ingressProducerStore.resolve(request.host(), request.path());
    if (producer == null) {
      request.response().setStatusCode(NOT_FOUND.code()).end();
      logger.warn("Resource not found {} {} {}",
          keyValue("path", request.path()),
          keyValue("host", request.host()),
          keyValue("hostHeader", request.getHeader("Host")));
      return;
    }

    if (isControlPlaneProbeRequest(request)) {
      request.response()
          .putHeader(PROBE_HASH_HEADER_NAME, request.getHeader(PROBE_HASH_HEADER_NAME))
          .setStatusCode(OK.code()).end();
      return;
    }

    // Invoke the ingress request handler
    this.ingressRequestHandler.handle(requestContext, producer);
  }
}
