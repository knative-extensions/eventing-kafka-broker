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
package dev.knative.eventing.kafka.broker.dispatcher.main;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

import dev.knative.eventing.kafka.broker.core.reconciler.EgressContext;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.MessageConsumer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This verticle listens on Egress reconciliations by deploying/undeploying new consumer verticles.
 */
public final class ConsumerDeployerVerticle extends AbstractVerticle implements EgressReconcilerListener {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDeployerVerticle.class);

    private final Map<String, String> deployedDispatchers;
    private final ConsumerVerticleFactory consumerFactory;

    private MessageConsumer<Object> messageConsumer;

    /**
     * All args constructor.
     *
     * @param consumerFactory         consumer factory.
     * @param egressesInitialCapacity egresses container initial capacity.
     */
    public ConsumerDeployerVerticle(final ConsumerVerticleFactory consumerFactory, final int egressesInitialCapacity) {
        Objects.requireNonNull(consumerFactory, "provide consumer factory");
        if (egressesInitialCapacity <= 0) {
            throw new IllegalArgumentException("egressesInitialCapacity cannot be negative or 0");
        }
        this.consumerFactory = consumerFactory;
        this.deployedDispatchers = new ConcurrentHashMap<>(egressesInitialCapacity);
    }

    @Override
    public void start() {
        this.messageConsumer = ResourcesReconciler.builder().watchEgress(this).buildAndListen(vertx);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        this.messageConsumer.unregister().onComplete(stopPromise);
    }

    @Override
    public Future<Void> onNewEgress(final EgressContext egressContext) {
        // TODO we should check if the consumer is still running
        if (this.deployedDispatchers.containsKey(egressContext.egress().getUid())) {
            return Future.succeededFuture();
        }

        try {
            AbstractVerticle verticle = consumerFactory.get(egressContext);

            final var deploymentOptions = new DeploymentOptions().setWorker(true);

            return vertx.deployVerticle(verticle, deploymentOptions)
                    .onSuccess(deploymentId -> {
                        this.deployedDispatchers.put(egressContext.egress().getUid(), deploymentId);
                        logger.info(
                                "Verticle deployed {} {} {}",
                                keyValue("egress.uid", egressContext.egress().getUid()),
                                keyValue(
                                        "resource.uid", egressContext.resource().getUid()),
                                keyValue("deploymentId", deploymentId));
                    })
                    .onFailure(cause -> {
                        // this is a bad state we cannot start the verticle for consuming messages.
                        logger.error(
                                "failed to start verticle {} {}",
                                keyValue("egress", egressContext.egress()),
                                keyValue("resource", egressContext.resource()),
                                cause);
                    })
                    .mapEmpty();
        } catch (Exception e) {
            logger.error(
                    "Potential control-plane bug: failed to get verticle {} {}",
                    keyValue("egress.uid", egressContext.egress().getUid()),
                    keyValue("resource.uid", egressContext.resource().getUid()),
                    e);
            return Future.failedFuture(
                    new IllegalStateException("Potential control-plane bug: failed to get verticle", e));
        }
    }

    @Override
    public Future<Void> onUpdateEgress(final EgressContext egressContext) {
        return onDeleteEgress(egressContext).compose(v -> onNewEgress(egressContext));
    }

    @Override
    public Future<Void> onDeleteEgress(final EgressContext egressContext) {
        if (!this.deployedDispatchers.containsKey(egressContext.egress().getUid())) {
            return Future.succeededFuture();
        }

        try {
            return vertx.undeploy(
                            this.deployedDispatchers.get(egressContext.egress().getUid()))
                    .compose(
                            v -> {
                                this.deployedDispatchers.remove(
                                        egressContext.egress().getUid());
                                logger.info(
                                        "Removed egress {} {}",
                                        keyValue(
                                                "egress.uid",
                                                egressContext.egress().getUid()),
                                        keyValue(
                                                "resource.uid",
                                                egressContext.resource().getUid()));
                                return Future.succeededFuture();
                            },
                            cause -> {
                                // IllegalStateException is thrown when a verticle is already un-deployed.
                                if (cause instanceof IllegalStateException) {
                                    this.deployedDispatchers.remove(
                                            egressContext.egress().getUid());
                                    return Future.succeededFuture();
                                }
                                logger.error(
                                        "Failed to un-deploy verticle {} {}",
                                        keyValue(
                                                "egress.uid",
                                                egressContext.egress().getUid()),
                                        keyValue(
                                                "resource.uid",
                                                egressContext.resource().getUid()),
                                        cause);
                                return Future.failedFuture(cause);
                            });
        } catch (final Exception ex) {
            return Future.failedFuture(ex);
        }
    }
}
