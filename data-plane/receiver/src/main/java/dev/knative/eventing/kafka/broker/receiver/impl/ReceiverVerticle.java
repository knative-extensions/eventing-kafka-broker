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

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;
import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.PROBE_HASH_HEADER_NAME;
import static dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil.isControlPlaneProbeRequest;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import dev.knative.eventing.kafka.broker.receiver.IngressRequestHandler;
import dev.knative.eventing.kafka.broker.receiver.RequestContext;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.AuthVerifierImpl;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.OIDCDiscoveryConfigListener;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.AuthHandler;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.MethodNotAllowedHandler;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.ProbeHandler;
import dev.knative.eventing.kafka.broker.receiver.main.ReceiverEnv;
import io.vertx.core.*;
import io.vertx.core.buffer.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.SSLOptions;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This verticle is responsible for implementing the logic of the receiver.
 *
 * <p>
 * The receiver is the component responsible for mapping incoming {@link
 * io.cloudevents.CloudEvent} requests to specific Kafka topics. In order to do
 * so, this component:
 *
 * <ul>
 * <li>Starts two {@link HttpServer}, one with http, and one with https,
 * listening for incoming
 * events
 * <li>Starts a {@link ResourcesReconciler}, listen on the event bus for
 * reconciliation events and
 * keeps track of the {@link
 * dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Ingress} objects
 * and their
 * {@code path => (topic, producer)} mapping
 * <li>Implements a request handler that invokes a series of {@code preHandlers}
 * (which are
 * assumed to complete synchronously) and then a final
 * {@link IngressRequestHandler} to
 * publish the record to Kafka
 * </ul>
 */
public class ReceiverVerticle extends AbstractVerticle implements Handler<HttpServerRequest> {

    private static final Logger logger = LoggerFactory.getLogger(ReceiverVerticle.class);
    private final File tlsKeyFile;
    private final File tlsCrtFile;
    private final File secretVolume;

    private final HttpServerOptions httpServerOptions;
    private final HttpServerOptions httpsServerOptions;
    private final Function<Vertx, IngressProducerReconcilableStore> ingressProducerStoreFactory;

    private final IngressRequestHandler ingressRequestHandler;
    private final ReceiverEnv env;

    private AuthHandler authHandler;
    private final Handler<HttpServerRequest> handler;
    private HttpServer httpServer;
    private HttpServer httpsServer;
    private MessageConsumer<Object> messageConsumer;
    private IngressProducerReconcilableStore ingressProducerStore;
    private FileWatcher secretWatcher;

    private final AuthVerifierImpl authVerifier;

    public ReceiverVerticle(
            final ReceiverEnv env,
            final HttpServerOptions httpServerOptions,
            final HttpServerOptions httpsServerOptions,
            final Function<Vertx, IngressProducerReconcilableStore> ingressProducerStoreFactory,
            final IngressRequestHandler ingressRequestHandler,
            final String secretVolumePath,
            final OIDCDiscoveryConfigListener oidcDiscoveryConfigListener) {

        Objects.requireNonNull(env);
        Objects.requireNonNull(httpServerOptions);
        Objects.requireNonNull(httpsServerOptions);
        Objects.requireNonNull(ingressProducerStoreFactory);
        Objects.requireNonNull(ingressRequestHandler);
        Objects.requireNonNull(secretVolumePath);

        this.env = env;
        this.httpServerOptions = httpServerOptions != null ? httpServerOptions : new HttpServerOptions();
        this.httpsServerOptions = httpsServerOptions;
        this.ingressProducerStoreFactory = ingressProducerStoreFactory;
        this.ingressRequestHandler = ingressRequestHandler;
        this.secretVolume = new File(secretVolumePath);
        this.tlsKeyFile = new File(secretVolumePath + "/tls.key");
        this.tlsCrtFile = new File(secretVolumePath + "/tls.crt");

        this.authVerifier = new AuthVerifierImpl(oidcDiscoveryConfigListener);
        this.authHandler = new AuthHandler(this.authVerifier);

        this.handler = new ProbeHandler(
                env.getLivenessProbePath(), env.getReadinessProbePath(), new MethodNotAllowedHandler(this));
    }

    public HttpServerOptions getHttpsServerOptions() {
        return httpsServerOptions;
    }

    @Override
    public void start(final Promise<Void> startPromise) {
        this.ingressProducerStore = this.ingressProducerStoreFactory.apply(vertx);
        this.messageConsumer = ResourcesReconciler.builder()
                .watchIngress(IngressReconcilerListener.all(this.ingressProducerStore, this.ingressRequestHandler))
                .buildAndListen(vertx);

        this.httpServer = vertx.createHttpServer(this.httpServerOptions);

        // check whether the secret volume is mounted
        if (secretVolume.exists()) {
            // The secret volume is mounted, we should start the https server
            // check whether the tls.key and tls.crt files exist

            if (this.tlsKeyFile.exists() && this.tlsCrtFile.exists() && httpsServerOptions != null) {
                PemKeyCertOptions keyCertOptions =
                        new PemKeyCertOptions().setKeyPath(tlsKeyFile.getPath()).setCertPath(tlsCrtFile.getPath());
                this.httpsServerOptions.setSsl(true).setPemKeyCertOptions(keyCertOptions);

                this.httpsServer = vertx.createHttpServer(this.httpsServerOptions);
            }
        }

        authVerifier.start(vertx);

        if (this.httpsServer != null) {
            CompositeFuture.all(
                            this.httpServer
                                    .requestHandler(handler)
                                    .exceptionHandler(startPromise::tryFail)
                                    .listen(this.httpServerOptions.getPort(), this.httpServerOptions.getHost()),
                            this.httpsServer
                                    .requestHandler(handler)
                                    .exceptionHandler(startPromise::tryFail)
                                    .listen(this.httpsServerOptions.getPort(), this.httpsServerOptions.getHost()))
                    .<Void>mapEmpty()
                    .onComplete(startPromise);
        } else {
            this.httpServer
                    .requestHandler(handler)
                    .exceptionHandler(startPromise::tryFail)
                    .listen(this.httpServerOptions.getPort(), this.httpServerOptions.getHost())
                    .<Void>mapEmpty()
                    .onComplete(startPromise);
        }

        setupSecretWatcher();
    }

    // Set up the secret watcher
    private void setupSecretWatcher() {
        try {
            this.secretWatcher = new FileWatcher(this.tlsCrtFile, this::updateServerConfig);
            this.secretWatcher.start();
        } catch (IOException e) {
            logger.error("Failed to start SecretWatcher", e);
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        CompositeFuture.all(
                        (this.httpServer != null ? this.httpServer.close().mapEmpty() : Future.succeededFuture()),
                        (this.httpsServer != null ? this.httpsServer.close().mapEmpty() : Future.succeededFuture()),
                        (this.messageConsumer != null ? this.messageConsumer.unregister() : Future.succeededFuture()))
                .<Void>mapEmpty()
                .onComplete(stopPromise);

        this.authVerifier.stop();

        // close the watcher
        if (this.secretWatcher != null) {
            try {
                this.secretWatcher.close();
            } catch (IOException e) {
                logger.error("Failed to close SecretWatcher", e);
            }
        }
    }

    @Override
    public void handle(final HttpServerRequest request) {
        // Look up for the ingress producer
        IngressProducer producer = this.ingressProducerStore.resolve(request.host(), request.path());
        if (producer == null) {
            request.response().setStatusCode(NOT_FOUND.code()).end();
            logger.warn(
                    "Resource not found {} {} {}",
                    keyValue("path", request.path()),
                    keyValue("host", request.host()),
                    keyValue("hostHeader", request.getHeader("Host")));
            return;
        }

        if (isControlPlaneProbeRequest(request)) {
            request.response()
                    .putHeader(PROBE_HASH_HEADER_NAME, request.getHeader(PROBE_HASH_HEADER_NAME))
                    .setStatusCode(OK.code())
                    .end();
            return;
        }

        RequestContext requestContext = new RequestContext(request);
        this.authHandler.handle(requestContext, producer, this.ingressRequestHandler);
    }

    public void updateServerConfig() {

        // This function will be called when the secret volume is updated

        // Check whether the tls.key and tls.crt files exist
        if (this.tlsKeyFile.exists() && this.tlsCrtFile.exists() && httpsServerOptions != null) {
            try {
                // Update SSL configuration by passing the new value of the certificate and key
                // Have to use value instead of path here otherwise the changes won't be applied
                final var keyCertOptions = new PemKeyCertOptions()
                        .setCertValue(Buffer.buffer(java.nio.file.Files.readString(this.tlsCrtFile.toPath())))
                        .setKeyValue(Buffer.buffer(java.nio.file.Files.readString(this.tlsKeyFile.toPath())));

                if (httpsServer == null) {
                    // receiver was started without an initialized HTTPS server --> initialize and start it now
                    httpsServerOptions.setSsl(true).setPemKeyCertOptions(keyCertOptions);
                    httpsServer = vertx.createHttpServer(httpsServerOptions);

                    this.httpsServer
                            .requestHandler(handler)
                            .exceptionHandler(e -> logger.error("Socket error in HTTPS server", e))
                            .listen(this.httpsServerOptions.getPort(), this.httpsServerOptions.getHost());
                } else {
                    httpsServer
                            .updateSSLOptions(new SSLOptions().setKeyCertOptions(keyCertOptions))
                            .onSuccess(v -> logger.info("Succeeded to update TLS key pair"))
                            .onFailure(e ->
                                    logger.error("Failed to update TLS key pair while executing updateSSLOptions", e));
                }
            } catch (IOException e) {
                logger.error("Failed to read file {}", tlsCrtFile.toPath(), e);
            }
        } else {
            if (httpsServer != null) {
                // We had a running HTTPS server before and TLS files were removed now --> shutdown HTTPS server again
                httpsServer.close();
                httpsServer = null;
            }
        }
    }
}
