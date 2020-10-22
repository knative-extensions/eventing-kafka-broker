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

package dev.knative.eventing.kafka.broker.dispatcher;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResourcesManager manages Resource and Egress objects by instantiating and starting verticles based on resources
 * configurations.
 *
 * <p>Note: {@link ConsumerDeployer} is not thread-safe and it's not supposed to be shared between
 * threads.
 */
public final class ConsumerDeployer implements EgressReconcilerListener {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerDeployer.class);

  private final Map<String, String> deployedDispatchers;

  private final Vertx vertx;
  private final ConsumerVerticleFactory consumerFactory;

  /**
   * All args constructor.
   *
   * @param vertx                   vertx instance.
   * @param consumerFactory         consumer factory.
   * @param egressesInitialCapacity egresses container initial capacity.
   */
  public ConsumerDeployer(
    final Vertx vertx,
    final ConsumerVerticleFactory consumerFactory,
    final int egressesInitialCapacity) {
    Objects.requireNonNull(vertx, "provide vertx instance");
    Objects.requireNonNull(consumerFactory, "provide consumer factory");
    if (egressesInitialCapacity <= 0) {
      throw new IllegalArgumentException("egressesInitialCapacity cannot be negative or 0");
    }
    this.vertx = vertx;
    this.consumerFactory = consumerFactory;
    this.deployedDispatchers = new HashMap<>(egressesInitialCapacity);
  }

  @Override
  public Future<Void> onNewEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    return consumerFactory.get(resource, egress)
      .onFailure(cause -> {
        // probably configuration are wrong, so do not retry.
        logger.error("potential control-plane bug: failed to get verticle {} {}",
          keyValue("egress", egress),
          keyValue("resource", resource),
          cause
        );
      })
      .compose(vertx::deployVerticle)
      .onSuccess(deploymentId -> {
        this.deployedDispatchers.put(egress.getUid(), deploymentId);
        logger.info("Verticle deployed {} {} {}",
          keyValue("egress", egress),
          keyValue("resource", resource),
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
      })
      .mapEmpty();
  }

  @Override
  public Future<Void> onUpdateEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    return onDeleteEgress(resource, egress)
      .compose(v -> onNewEgress(resource, egress));
  }

  @Override
  public Future<Void> onDeleteEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    return vertx.undeploy(this.deployedDispatchers.remove(egress.getUid()))
      .onSuccess(v -> logger.info(
        "Removed egress {} {}",
        keyValue("egress", egress),
        keyValue("resource", resource)
      ))
      .onFailure(cause -> logger.error(
        "failed to un-deploy verticle {} {}",
        keyValue("egress", egress),
        keyValue("resource", resource),
        cause
      ));
  }
}
