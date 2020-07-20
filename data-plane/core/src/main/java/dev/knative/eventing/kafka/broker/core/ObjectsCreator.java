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

import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
import io.cloudevents.CloudEvent;
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
public class ObjectsCreator implements Consumer<Brokers> {

  private static final Logger logger = LoggerFactory.getLogger(ObjectsCreator.class);

  private static final int WAIT_TIMEOUT = 1;

  private final ObjectsReconciler<CloudEvent> objectsReconciler;

  /**
   * All args constructor.
   *
   * @param objectsReconciler brokers and triggers consumer.
   */
  public ObjectsCreator(final ObjectsReconciler<CloudEvent> objectsReconciler) {
    Objects.requireNonNull(objectsReconciler, "provide objectsReconciler");

    this.objectsReconciler = objectsReconciler;
  }

  /**
   * Capture new changes.
   *
   * @param brokers new brokers config.
   */
  @Override
  public void accept(final Brokers brokers) {

    final Map<Broker, Set<Trigger<CloudEvent>>> objects = new HashMap<>();

    for (final var broker : brokers.getBrokerList()) {
      final var triggers = new HashSet<Trigger<CloudEvent>>(
          broker.getTriggersCount()
      );
      for (final var trigger : broker.getTriggersList()) {
        triggers.add(new TriggerWrapper(trigger));
      }

      objects.put(new BrokerWrapper(broker), triggers);
    }

    try {
      final var latch = new CountDownLatch(1);
      objectsReconciler.reconcile(objects).onComplete(result -> {
        if (result.succeeded()) {
          logger.info("reconciled objects {}", brokers);
        } else {
          logger.error("failed to reconcile {}", brokers);
        }
        latch.countDown();
      });

      // wait the reconcilation
      latch.await(WAIT_TIMEOUT, TimeUnit.MINUTES);

    } catch (final Exception ex) {
      logger.error("failed to reconcile objects - cause {} - objects {}", ex, objects);
    }
  }
}
