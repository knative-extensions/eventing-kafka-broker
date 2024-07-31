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
package dev.knative.eventing.kafka.broker.receiver.main;

import dev.knative.eventing.kafka.broker.core.ReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.eventtype.EventType;
import dev.knative.eventing.kafka.broker.core.features.FeaturesConfig;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.oidc.OIDCDiscoveryConfig;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.utils.Shutdown;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiverDeployer {

    private final ReactiveProducerFactory kafkaProducerFactory;
    private final ReceiverEnv env;
    private final Properties producerConfigs;
    private final Vertx vertx;
    private final HttpServerOptions httpServerOptions;
    private final HttpServerOptions httpsServerOptions;
    private final MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient;
    private final SharedIndexInformer<EventType> eventTypeInformer;
    private FeaturesConfig featuresConfig;
    private OIDCDiscoveryConfig oidcDiscoveryConfig;
    private final OpenTelemetrySdk openTelemetry;
    private String deploymentId;
    private FileWatcher featuresConfigWatcher;

    private static final Logger logger = LoggerFactory.getLogger(ReceiverDeployer.class);

    public ReceiverDeployer(
            final ReactiveProducerFactory kafkaProducerFactory,
            final ReceiverEnv env,
            final Properties producerConfigs,
            final Vertx vertx,
            final HttpServerOptions httpServerOptions,
            final HttpServerOptions httpsServerOptions,
            final MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient,
            final SharedIndexInformer<EventType> eventTypeInformer,
            OpenTelemetrySdk openTelemetry)
            throws IOException, ExecutionException, InterruptedException {
        this.kafkaProducerFactory = kafkaProducerFactory;
        this.env = env;
        this.producerConfigs = producerConfigs;
        this.vertx = vertx;
        this.httpServerOptions = httpServerOptions;
        this.httpsServerOptions = httpsServerOptions;
        this.eventTypeClient = eventTypeClient;
        this.eventTypeInformer = eventTypeInformer;
        this.featuresConfig = new FeaturesConfig(env.getConfigFeaturesPath());
        this.openTelemetry = openTelemetry;
        this.featuresConfigWatcher = new FileWatcher(
                new File(env.getConfigFeaturesPath() + "/" + FeaturesConfig.KEY_AUTHENTICATION_OIDC), () -> {
                    logger.info("config features updated, reloading");
                    try {
                        this.featuresConfig = new FeaturesConfig(env.getConfigFeaturesPath());
                        if (this.featuresConfig.isAuthenticationOIDC() && this.oidcDiscoveryConfig == null) {
                            logger.info("building OIDC config");
                            this.buildOIDCDiscoveryConfig();
                            logger.info("undeploying verticles");
                            this.undeploy();
                            logger.info("deploying verticles again with new oidc config");
                            this.deploy();
                        }
                    } catch (IOException
                            | ExecutionException
                            | InterruptedException
                            | NoSuchAlgorithmException
                            | TimeoutException e) {
                        logger.warn("features config file updated but was unable to rebuild the config", e);
                    }
                });
        if (this.featuresConfig.isAuthenticationOIDC()) {
            this.buildOIDCDiscoveryConfig();
        }
    }

    public void buildOIDCDiscoveryConfig() throws ExecutionException, InterruptedException {
        try {
            this.oidcDiscoveryConfig = OIDCDiscoveryConfig.build(this.vertx)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(env.getWaitStartupSeconds(), TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.error("Could not load OIDC config with OIDC authentication feature is enabled.", ex);
            Shutdown.closeVertxSync(vertx);
            System.exit(1);
        }
    }

    public void run() {
        try {
            this.featuresConfigWatcher.start();
        } catch (IOException ex) {
            logger.warn("failed to start features config watcher");
        }

        try {
            // deploy the receiver verticles
            this.deploymentId = this.deploy();
            logger.info("Receiver started");

            // set up contract publisher
            ContractPublisher publisher =
                    new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);
            File file = new File(env.getDataPlaneConfigFilePath());
            FileWatcher fileWatcher = new FileWatcher(file, () -> publisher.updateContract(file));
            fileWatcher.start();

            var closeables =
                    new ArrayList<>(Arrays.asList(publisher, fileWatcher, openTelemetry.getSdkTracerProvider()));

            if (eventTypeInformer != null) {
                closeables.add(eventTypeInformer);
            }

            // Register shutdown hook for graceful shutdown.
            Shutdown.registerHook(vertx, closeables.toArray(new AutoCloseable[0]));
        } catch (final Exception ex) {
            logger.error("Failed to startup the receiver", ex);
            Shutdown.closeVertxSync(vertx);
            System.exit(1);
        }
    }

    private String deploy()
            throws ExecutionException, InterruptedException, TimeoutException, NoSuchAlgorithmException {
        final Supplier<Verticle> receiverVerticleFactory = new ReceiverVerticleFactory(
                env,
                producerConfigs,
                Metrics.getRegistry(),
                httpServerOptions,
                httpsServerOptions,
                kafkaProducerFactory,
                eventTypeClient,
                eventTypeInformer,
                vertx,
                oidcDiscoveryConfig);
        DeploymentOptions deploymentOptions =
                new DeploymentOptions().setInstances(Runtime.getRuntime().availableProcessors());
        // Deploy the receiver verticles
        return vertx.deployVerticle(receiverVerticleFactory, deploymentOptions)
                .toCompletionStage()
                .toCompletableFuture()
                .get(env.getWaitStartupSeconds(), TimeUnit.SECONDS);
    }

    private void undeploy() {
        if (this.deploymentId == null || this.deploymentId.isEmpty()) {
            return;
        }

        this.vertx.undeploy(this.deploymentId);
    }
}
