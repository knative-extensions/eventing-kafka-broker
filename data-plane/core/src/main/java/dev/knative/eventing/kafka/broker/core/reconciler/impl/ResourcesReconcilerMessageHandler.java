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
package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ResourcesReconcilerMessageHandler implements Handler<Message<Object>> {

  private static final Logger logger = LoggerFactory.getLogger(ResourcesReconcilerMessageHandler.class);

  public final static String ADDRESS = "resourcesreconciler.core";

  private final ResourcesReconciler resourcesReconciler;
  private final AtomicBoolean reconciling;
  private final AtomicReference<DataPlaneContract.Contract> last;

  public ResourcesReconcilerMessageHandler(final ResourcesReconciler resourcesReconciler) {
    this.resourcesReconciler = resourcesReconciler;
    reconciling = new AtomicBoolean();
    last = new AtomicReference<>();
  }

  @Override
  public void handle(Message<Object> event) {
    reconcileLast((DataPlaneContract.Contract) event.body());
  }

  private void reconcileLast(final DataPlaneContract.Contract newContract) {

    if (newContract != null) {
      last.set(newContract);

      logger.debug("Set new contract {}", newContract.getGeneration());
    }

    // Our reconciler is on the same verticle of the handler, therefore they use both the same thread.
    // However, if the reconciler makes an async request or executes a blocking operation, a new contract message to
    // reconcile might be scheduled to our verticle thread, that might generate inconsistencies.
    if (reconciling.compareAndSet(false, true)) {

      final var contract = last.get();

      logger.debug("Reconciling contract {}", contract.getGeneration());

      resourcesReconciler.reconcile(contract.getResourcesList())
        .onSuccess(v -> {
          MDC.put("contractGeneration", String.valueOf(contract.getGeneration()));
          logger.info(
            "Reconciled contract generation {}", contract.getGeneration()
          );
          MDC.clear();
        })
        .onFailure(cause -> {
          MDC.put("contractGeneration", String.valueOf(contract.getGeneration()));
          logger.error(
            "Failed to reconcile contract generation {}",
            contract.getGeneration(),
            cause
          );
          MDC.clear();
        })
        .onComplete(r -> {

          // We have reconciled the last known contract.
          reconciling.set(false);

          // During a reconcile a new contract might have been set.
          // If that's the case, reconcile it.
          if (last.get().getGeneration() != contract.getGeneration()) {
            reconcileLast(null);
          }
        });
    }
  }

  public static MessageConsumer<Object> start(final Vertx vertx, final ResourcesReconciler reconciler) {
    return vertx.eventBus().localConsumer(ADDRESS, new ResourcesReconcilerMessageHandler(reconciler));
  }
}
