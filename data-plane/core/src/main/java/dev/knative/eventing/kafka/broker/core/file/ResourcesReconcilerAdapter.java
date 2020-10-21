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

package dev.knative.eventing.kafka.broker.core.file;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class adapts the {@link dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Contract} received from the {@link FileWatcher}
 * and invokes the provided {@link ResourcesReconciler}
 */
public class ResourcesReconcilerAdapter implements Consumer<DataPlaneContract.Contract> {

  private static final Logger logger = LoggerFactory.getLogger(ResourcesReconcilerAdapter.class);

  private static final int WAIT_TIMEOUT = 1;

  private final ResourcesReconciler resourcesReconciler;

  /**
   * All args constructor.
   *
   * @param resourcesReconciler brokers and triggers consumer.
   */
  public ResourcesReconcilerAdapter(final ResourcesReconciler resourcesReconciler) {
    Objects.requireNonNull(resourcesReconciler, "provide objectsReconciler");

    this.resourcesReconciler = resourcesReconciler;
  }

  /**
   * Capture new changes.
   *
   * @param contract new contract config.
   */
  @Override
  public void accept(final DataPlaneContract.Contract contract) {
    try {
      final var latch = new CountDownLatch(1);
      resourcesReconciler.reconcile(contract.getResourcesList())
        .onComplete(result -> {
          if (result.succeeded()) {
            logger.info("reconciled objects {}", keyValue("contract", contract));
          } else {
            logger.error("failed to reconcile {}", keyValue("contract", contract), result.cause());
          }

          latch.countDown();
        });

      // wait the reconciliation
      latch.await(WAIT_TIMEOUT, TimeUnit.MINUTES);

    } catch (final Exception ex) {
      logger.error("{}", keyValue("contract", contract), ex);
    }
  }
}
