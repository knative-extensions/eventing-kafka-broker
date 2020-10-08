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

import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.core.wrappers.Egress;
import dev.knative.eventing.kafka.broker.core.wrappers.Resource;
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
 * ResourcesManager manages Resource and Egress objects by instantiating and starting verticles based on resources
 * configurations.
 *
 * <p>Note: {@link ResourcesManager} is not thread-safe and it's not supposed to be shared between
 * threads.
 */
public final class ResourcesManager implements ResourcesReconciler {

  private static final Logger logger = LoggerFactory.getLogger(ResourcesManager.class);

  // Resource -> Egress -> AbstractVerticle
  private final Map<Resource, Map<Egress, AbstractVerticle>> resources;

  private final Vertx vertx;
  private final ConsumerVerticleFactory consumerFactory;
  private final int egressesInitialCapacity;

  /**
   * All args constructor.
   *
   * @param vertx                    vertx instance.
   * @param consumerFactory          consumer factory.
   * @param resourcesInitialCapacity resources container initial capacity.
   * @param egressesInitialCapacity  egresses container initial capacity.
   */
  public ResourcesManager(
    final Vertx vertx,
    final ConsumerVerticleFactory consumerFactory,
    final int resourcesInitialCapacity,
    final int egressesInitialCapacity) {

    Objects.requireNonNull(vertx, "provide vertx instance");
    Objects.requireNonNull(consumerFactory, "provide consumer factory");
    if (resourcesInitialCapacity <= 0) {
      throw new IllegalArgumentException("resourcesInitialCapacity cannot be negative or 0");
    }
    if (egressesInitialCapacity <= 0) {
      throw new IllegalArgumentException("egressesInitialCapacity cannot be negative or 0");
    }
    this.vertx = vertx;
    this.consumerFactory = consumerFactory;
    this.egressesInitialCapacity = egressesInitialCapacity;
    resources = new HashMap<>(resourcesInitialCapacity);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("rawtypes")
  public Future<Void> reconcile(Map<Resource, Set<Egress>> resourcesMap) {
    final List<Future> futures = new ArrayList<>(resourcesMap.size() * 2);

    // diffing previous and new --> remove deleted objects
    final var resourcesIterator = resources.entrySet().iterator();
    while (resourcesIterator.hasNext()) {
      final var resourceEntry = resourcesIterator.next();
      final var oldResource = resourceEntry.getKey();

      // check if the old resource has been deleted or updated.
      if (!resourcesMap.containsKey(oldResource)) {

        // resource deleted or updated, so remove it
        resourcesIterator.remove();

        // undeploy all verticles associated with egresses of the deleted resource.
        for (final var e : resourceEntry.getValue().entrySet()) {
          futures.add(undeployVerticle(oldResource, e.getKey(), e.getValue()));
        }

        continue;
      }

      // resource is there, so check if some egresses have been deleted.
      final var egressesIterator = resourceEntry.getValue().entrySet().iterator();
      while (egressesIterator.hasNext()) {
        final var egressEntry = egressesIterator.next();

        // check if the egress has been deleted or updated.
        if (!resourcesMap.get(oldResource).contains(egressEntry.getKey())) {

          // egress deleted or updated, so remove it
          egressesIterator.remove();

          // undeploy verticle associated with the deleted egress.
          futures.add(undeployVerticle(
            oldResource,
            egressEntry.getKey(),
            egressEntry.getValue()
          ));
        }

      }
    }

    // add all new objects.
    for (final var entry : resourcesMap.entrySet()) {
      final var resource = entry.getKey();
      for (final var egress : entry.getValue()) {
        futures.add(addResource(resource, egress));
      }
    }

    return CompositeFuture.all(futures).mapEmpty();
  }

  private Future<Void> addResource(final Resource resource, final Egress egress) {
    final Map<Egress, AbstractVerticle> egresses =
      resources.computeIfAbsent(resource, v -> new ConcurrentHashMap<>(egressesInitialCapacity));

    if (egress == null || egresses.containsKey(egress)) {
      // the trigger is already there and it hasn't been updated.
      return Future.succeededFuture();
    }

    return consumerFactory.get(resource, egress)
      .onFailure(cause -> {

        // probably configuration are wrong, so do not retry.
        logger.error("potential control-plane bug: failed to get verticle {} {}",
          keyValue("egress", egress),
          keyValue("resource", resource),
          cause
        );

      })
      .compose(verticle -> deployVerticle(verticle, resource, egresses, egress));
  }

  private Future<Void> deployVerticle(
    final AbstractVerticle verticle,
    final Resource resource,
    final Map<Egress, AbstractVerticle> egresses,
    final Egress egress) {
    egresses.put(egress, verticle);
    return vertx.deployVerticle(verticle)
      .onSuccess(msg -> {
        logger.info("Verticle deployed {} {} {}",
          keyValue("egress", egress),
          keyValue("resource", resource),
          keyValue("message", msg)
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

  private Future<Void> undeployVerticle(
    Resource resource,
    Egress egress,
    AbstractVerticle verticle) {
    return vertx.undeploy(verticle.deploymentID())
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
