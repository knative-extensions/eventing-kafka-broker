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
package dev.knative.eventing.kafka.broker.dispatcher;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerImpl;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.MessageConsumer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResourcesManager manages Resource and Egress objects by instantiating and starting verticles based on resources
 * configurations.
 *
 * <p>Note: {@link ConsumerDeployerVerticle} is not thread-safe and it's not supposed to be shared between
 * threads.
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
  public ConsumerDeployerVerticle(
    final ConsumerVerticleFactory consumerFactory,
    final int egressesInitialCapacity) {
    Objects.requireNonNull(consumerFactory, "provide consumer factory");
    if (egressesInitialCapacity <= 0) {
      throw new IllegalArgumentException("egressesInitialCapacity cannot be negative or 0");
    }
    this.consumerFactory = consumerFactory;
    this.deployedDispatchers = new HashMap<>(egressesInitialCapacity);
  }

  @Override
  public void start() {
    this.messageConsumer = ResourcesReconcilerMessageHandler.start(
      vertx.eventBus(),
      ResourcesReconcilerImpl
        .builder()
        .watchEgress(this)
        .build()
    );
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    this.messageConsumer.unregister().onComplete(stopPromise);
  }

  @Override
  public Future<Void> onNewEgress(final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {
    try {
      AbstractVerticle verticle = consumerFactory.get(resource, egress);

      logger.info("Deploying verticle onNewEgress {} {}",
        keyValue("resource.uid", resource.getUid()),
        keyValue("egress.uid", egress.getUid())
      );

      return vertx.deployVerticle(verticle)
        .onSuccess(deploymentId -> {
          this.deployedDispatchers.put(egress.getUid(), deploymentId);
          logger.info("Verticle deployed {} {} {}",
            keyValue("resource.uid", resource.getUid()),
            keyValue("egress.uid", egress.getUid()),
            keyValue("deploymentId", deploymentId)
          );
        })
        .onFailure(cause -> {
          // this is a bad state we cannot start the verticle for consuming messages.
          logger.error("failed to start verticle {} {}",
            keyValue("resource.uid", resource.getUid()),
            keyValue("egress.uid", egress.getUid()),
            cause
          );
        })
        .mapEmpty();
    } catch (Exception e) {
      logger.error("potential control-plane bug: failed to get verticle {} {}",
        keyValue("resource.uid", resource.getUid()),
        keyValue("egress.uid", egress.getUid()),
        e
      );
      return Future.failedFuture(new IllegalStateException("Potential control-plane bug: failed to get verticle", e));
    }
  }

  @Override
  public Future<Void> onUpdateEgress(final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {
    logger.info("onUpdateEgress {} {}",
      keyValue("resource.uid", resource.getUid()),
      keyValue("egress.uid", egress.getUid())
    );

    return onDeleteEgress(resource, egress)
      .compose(v -> onNewEgress(resource, egress));
  }

  @Override
  public Future<Void> onDeleteEgress(final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {
    final var deploymentID = this.deployedDispatchers.remove(egress.getUid());
    if (deploymentID == null) {
      logger.warn(
        "Verticle not found onDeleteEgress for resource {} {}",
        keyValue("resource.uid", resource.getUid()),
        keyValue("egress.uid", egress.getUid())
      );
      return Future.succeededFuture();
    }

    return vertx.undeploy(deploymentID)
      .onSuccess(v -> logger.info(
        "Removed egress {} {}",
        keyValue("resource.uid", resource.getUid()),
        keyValue("egress.uid", egress.getUid())
      ))
      .onFailure(cause -> logger.error(
        "Failed to un-deploy verticle {} {}",
        keyValue("resource.uid", resource.getUid()),
        keyValue("egress.uid", egress.getUid()),
        cause
      ));
  }
}
