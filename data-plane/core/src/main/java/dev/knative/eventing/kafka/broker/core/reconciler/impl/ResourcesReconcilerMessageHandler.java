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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

public class ResourcesReconcilerMessageHandler implements Handler<Message<Object>> {

  private static final Logger logger = LoggerFactory.getLogger(ResourcesReconcilerMessageHandler.class);

  public final static String ADDRESS = "resourcesreconciler.core";

  private final Vertx vertx;
  private final ResourcesReconciler resourcesReconciler;
  private final AtomicBoolean scheduled;
  private final AtomicBoolean reconciling;
  private final AtomicReference<DataPlaneContract.Contract> last;

  public ResourcesReconcilerMessageHandler(final Vertx vertx, final ResourcesReconciler resourcesReconciler) {
    this.vertx = vertx;
    this.resourcesReconciler = resourcesReconciler;
    reconciling = new AtomicBoolean();
    scheduled = new AtomicBoolean();
    last = new AtomicReference<>();
  }

  @Override
  public void handle(Message<Object> event) {
    DataPlaneContract.Contract contract = (DataPlaneContract.Contract) event.body();
    last.set(contract);
    reconcileLast();
  }

  private void reconcileLast() {
    if (reconciling.compareAndSet(false, true)) {
      final var contract = last.get();
      resourcesReconciler.reconcile(contract.getResourcesList())
        .onComplete(r -> reconciling.set(false))
        .onSuccess(v -> logger.info("reconciled contract generation {}", keyValue("contractGeneration", contract.getGeneration())))
        .onFailure(cause -> logger.error("failed to reconcile contract generation {}", keyValue("contractGeneration", contract.getGeneration()), cause));
    } else if (scheduled.compareAndSet(false, true)) {
      // We can't wait, so schedule a reconcile.
      vertx.setTimer(500, h -> {
        scheduled.set(false);
        reconcileLast();
      });
    }
  }

  public static MessageConsumer<Object> start(final Vertx vertx, ResourcesReconciler reconciler) {
    return vertx.eventBus().localConsumer(ADDRESS, new ResourcesReconcilerMessageHandler(vertx, reconciler));
  }
}
