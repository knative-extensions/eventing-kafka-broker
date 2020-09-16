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

package dev.knative.eventing.kafka.broker.core;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ObjectsCreator receives updates and converts protobuf objects to core objects often by wrapping
 * protobuf objects by means of wrapper objects.
 */
public class ObjectsCreator implements Consumer<DataPlaneContract.Contract> {

  private static final Logger logger = LoggerFactory.getLogger(ObjectsCreator.class);

  private static final int WAIT_TIMEOUT = 1;

  private final ObjectsReconciler objectsReconciler;

  /**
   * All args constructor.
   *
   * @param objectsReconciler brokers and triggers consumer.
   */
  public ObjectsCreator(final ObjectsReconciler objectsReconciler) {
    Objects.requireNonNull(objectsReconciler, "provide objectsReconciler");

    this.objectsReconciler = objectsReconciler;
  }

  /**
   * Capture new changes.
   *
   * @param contract new contract config.
   */
  @Override
  public void accept(final DataPlaneContract.Contract contract) {
    final Map<Resource, Set<Egress>> objects = new HashMap<>();

    for (final var resource : contract.getResourcesList()) {
      final var egresses = new HashSet<Egress>(
        resource.getEgressesCount()
      );
      for (final var egress : resource.getEgressesList()) {
        egresses.add(new EgressWrapper(egress));
      }

      objects.put(new ResourceWrapper(resource), egresses);
    }

    try {
      final var latch = new CountDownLatch(1);
      objectsReconciler.reconcile(objects)
        .onComplete(result -> {
          if (result.succeeded()) {
            logger.info("reconciled objects {}", keyValue("contract", contract));
          } else {
            logger.error("failed to reconcile {}", keyValue("contract", contract), result.cause());
          }

          latch.countDown();
        });

      // wait the reconcilation
      latch.await(WAIT_TIMEOUT, TimeUnit.MINUTES);

    } catch (final Exception ex) {
      logger.error("{}", keyValue("objects", objects), ex);
    }
  }
}
