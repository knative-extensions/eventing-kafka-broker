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

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.ObjectsReconciler;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Brokers manager manges Broker and Trigger objects by instantiating and starting verticles based
 * on brokers configurations.
 *
 * <p>Note: {@link BrokersManager} is not thread-safe and it's not supposed to be shared between
 * threads.
 *
 * @param <T> trigger type.
 */
public final class BrokersManager<T> implements ObjectsReconciler<T> {

  private static final Logger logger = LoggerFactory.getLogger(BrokersManager.class);

  // Broker -> Trigger<T> -> AbstractVerticle
  private final Map<Broker, Map<Trigger<T>, AbstractVerticle>> brokers;

  private final Vertx vertx;
  private final ConsumerVerticleFactory<T> consumerFactory;
  private final int triggersInitialCapacity;

  /**
   * All args constructor.
   *
   * @param vertx                   vertx instance.
   * @param consumerFactory         consumer factory.
   * @param brokersInitialCapacity  brokers container initial capacity.
   * @param triggersInitialCapacity triggers container initial capacity.
   */
  public BrokersManager(
    final Vertx vertx,
    final ConsumerVerticleFactory<T> consumerFactory,
    final int brokersInitialCapacity,
    final int triggersInitialCapacity) {

    Objects.requireNonNull(vertx, "provide vertx instance");
    Objects.requireNonNull(consumerFactory, "provide consumer factory");
    if (brokersInitialCapacity <= 0) {
      throw new IllegalArgumentException("brokersInitialCapacity cannot be negative or 0");
    }
    if (triggersInitialCapacity <= 0) {
      throw new IllegalArgumentException("triggersInitialCapacity cannot be negative or 0");
    }
    this.vertx = vertx;
    this.consumerFactory = consumerFactory;
    this.triggersInitialCapacity = triggersInitialCapacity;
    brokers = new HashMap<>(brokersInitialCapacity);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("rawtypes")
  public Future<Void> reconcile(final Map<Broker, Set<Trigger<T>>> newObjects) {
    final List<Future> futures = new ArrayList<>(newObjects.size() * 2);

    // diffing previous and new --> remove deleted objects
    for (final var brokersIt = brokers.entrySet().iterator(); brokersIt.hasNext(); ) {

      final var brokerTriggers = brokersIt.next();
      final var broker = brokerTriggers.getKey();

      // check if the old broker has been deleted or updated.
      if (!newObjects.containsKey(broker)) {

        // broker deleted or updated, so remove it
        brokersIt.remove();

        // undeploy all verticles associated with triggers of the deleted broker.
        for (final var e : brokerTriggers.getValue().entrySet()) {
          futures.add(undeployVerticle(broker, e.getKey(), e.getValue()));
        }

        continue;
      }

      // broker is there, so check if some triggers have been deleted.
      final var triggersVerticles = brokerTriggers.getValue();
      for (final var triggersIt = triggersVerticles.entrySet().iterator(); triggersIt.hasNext(); ) {

        final var triggerVerticle = triggersIt.next();

        // check if the trigger has been deleted or updated.
        if (!newObjects.get(broker).contains(triggerVerticle.getKey())) {

          // trigger deleted or updated, so remove it
          triggersIt.remove();

          // undeploy verticle associated with the deleted trigger.
          futures.add(undeployVerticle(
            broker,
            triggerVerticle.getKey(),
            triggerVerticle.getValue()
          ));
        }
      }
    }

    // add all new objects.
    for (final var entry : newObjects.entrySet()) {
      final var broker = entry.getKey();
      for (final var trigger : entry.getValue()) {
        futures.add(addBroker(broker, trigger));
      }
    }

    return CompositeFuture.all(futures).mapEmpty();
  }

  private Future<Void> addBroker(final Broker broker, final Trigger<T> trigger) {
    final Map<Trigger<T>, AbstractVerticle> triggers;

    if (brokers.containsKey(broker)) {
      triggers = brokers.get(broker);
    } else {
      triggers = new ConcurrentHashMap<>(triggersInitialCapacity);
      brokers.put(broker, triggers);
    }

    if (trigger == null || triggers.containsKey(trigger)) {
      // the trigger is already there and it hasn't been updated.
      return Future.succeededFuture();
    }

    return consumerFactory.get(broker, trigger)
      .onFailure(cause -> {

        // probably configuration are wrong, so do not retry.
        logger.error("potential control-plane bug: failed to get verticle {} {}",
          keyValue("trigger", trigger),
          keyValue("broker", broker),
          cause
        );

      })
      .compose(verticle -> deployVerticle(verticle, broker, triggers, trigger));
  }

  private Future<Void> deployVerticle(
    final AbstractVerticle verticle,
    final Broker broker,
    final Map<Trigger<T>, AbstractVerticle> triggers,
    final Trigger<T> trigger) {
    triggers.put(trigger, verticle);
    return vertx.deployVerticle(verticle)
      .onSuccess(msg -> {
        logger.info("Verticle deployed {} {} {}",
          keyValue("trigger", trigger),
          keyValue("broker", broker),
          keyValue("message", msg)
        );
      })
      .onFailure(cause -> {
        // this is a bad state we cannot start the verticle for consuming messages.
        logger.error("failed to start verticle {} {}",
          keyValue("trigger", trigger),
          keyValue("broker", broker),
          cause
        );
      })
      .mapEmpty();
  }

  private Future<Void> undeployVerticle(
    Broker broker,
    Trigger<T> trigger,
    AbstractVerticle verticle) {
    return vertx.undeploy(verticle.deploymentID())
      .onSuccess(v -> logger.info(
        "Removed trigger {} {}",
        keyValue("trigger", trigger),
        keyValue("broker", broker)
      ))
      .onFailure(cause -> logger.error(
        "failed to un-deploy verticle {} {}",
        keyValue("trigger", trigger),
        keyValue("broker", broker),
        cause
      ));
  }
}
