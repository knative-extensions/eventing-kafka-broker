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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerImpl;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

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
    // TODO we should check if the consumer is still running
    if (this.deployedDispatchers.containsKey(egress.getUid())) {
      return Future.succeededFuture();
    }

    try {
      AbstractVerticle verticle = consumerFactory.get(resource, egress);
      return vertx.deployVerticle(verticle)
        .onSuccess(deploymentId -> {
          this.deployedDispatchers.put(egress.getUid(), deploymentId);
          logger.info("Verticle deployed {} {} {}",
            keyValue("egress.uid", egress.getUid()),
            keyValue("resource.uid", resource.getUid()),
            keyValue("deploymentId", deploymentId)
          );
        })
        .onFailure(cause -> {
            // this is a bad state we cannot start the verticle for consuming messages.
            logger.error("failed to start verticle {} {}",
              keyValue("egress", egress),
              keyValue("resource", resource),
              cause
            );
          }
        )
        .mapEmpty();
    } catch (Exception e) {
      logger.error("Potential control-plane bug: failed to get verticle {} {}",
        keyValue("egress.uid", egress.getUid()),
        keyValue("resource.uid", resource.getUid()),
        e
      );
      return Future.failedFuture(new IllegalStateException("Potential control-plane bug: failed to get verticle", e));
    }
  }

  @Override
  public Future<Void> onUpdateEgress(final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {
    return onDeleteEgress(resource, egress)
      .compose(v -> onNewEgress(resource, egress));
  }

  @Override
  public Future<Void> onDeleteEgress(final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {
    if (!this.deployedDispatchers.containsKey(egress.getUid())) {
      return Future.succeededFuture();
    }

    return vertx.undeploy(this.deployedDispatchers.get(egress.getUid()))
      .compose(
        v -> {
          this.deployedDispatchers.remove(egress.getUid());
          logger.info(
            "Removed egress {} {}",
            keyValue("egress.uid", egress.getUid()),
            keyValue("resource.uid", resource.getUid())
          );
          return Future.succeededFuture();
        },
        cause -> {
          // IllegalStateException is thrown when a verticle is already un-deployed.
          if (cause instanceof IllegalStateException) {
            this.deployedDispatchers.remove(egress.getUid());
            return Future.succeededFuture();
          }
          logger.error(
            "Failed to un-deploy verticle {} {}",
            keyValue("egress.uid", egress.getUid()),
            keyValue("resource.uid", resource.getUid()),
            cause
          );
          return Future.failedFuture(cause);
        }
      );
  }
}
