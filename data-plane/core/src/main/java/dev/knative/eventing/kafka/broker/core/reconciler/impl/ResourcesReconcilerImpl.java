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
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.core.utils.CollectionsUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class ResourcesReconcilerImpl implements ResourcesReconciler {

  private static final Logger logger = LoggerFactory.getLogger(ResourcesReconcilerImpl.class);

  private static final int CACHED_RESOURCES_INITIAL_CAPACITY = 32;

  private final IngressReconcilerListener ingressReconcilerListener;
  private final EgressReconcilerListener egressReconcilerListener;

  // Every resource ingress is identified by its resource, so we don't need to store ingress separately
  private final Map<String, DataPlaneContract.Resource> cachedResources;
  // egress uid -> <Egress, Resource>
  private final Map<String, Map.Entry<DataPlaneContract.Egress, DataPlaneContract.Resource>> cachedEgresses;

  ResourcesReconcilerImpl(
    IngressReconcilerListener ingressReconcilerListener,
    EgressReconcilerListener egressReconcilerListener) {
    if (ingressReconcilerListener == null && egressReconcilerListener == null) {
      throw new NullPointerException("You need to specify at least one listener");
    }
    this.ingressReconcilerListener = ingressReconcilerListener;
    this.egressReconcilerListener = egressReconcilerListener;

    this.cachedResources = ingressReconcilerListener == null ? null : new ConcurrentHashMap<>(CACHED_RESOURCES_INITIAL_CAPACITY);
    this.cachedEgresses = egressReconcilerListener == null ? null : new ConcurrentHashMap<>(CACHED_RESOURCES_INITIAL_CAPACITY);
  }

  @Override
  public synchronized Future<Void> reconcile(final DataPlaneContract.Contract contract) {
    if (isReconcilingIngress()) {
      return reconcileIngress(contract);
    }
    return reconcileEgress(contract);
  }

  private Future<Void> reconcileEgress(final DataPlaneContract.Contract contract) {

    final var generation = contract.getGeneration();

    final var egresses = contract.getResourcesList().stream()
      .filter(r -> r.getEgressesCount() > 0)
      .flatMap(r -> r.getEgressesList()
        .stream()
        // egress uid -> <Egress, Resource>
        .map(e -> new SimpleImmutableEntry<>(e.getUid(), new SimpleImmutableEntry<>(e, r))))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final List<Future> futures = new ArrayList<>(egresses.size() + this.cachedEgresses.size());

    final var diff = CollectionsUtils.diff(this.cachedEgresses.keySet(), egresses.keySet());
    logger.info("Reconcile egress diff {} {}", keyValue("diff", diff), keyValue("contractGeneration", generation));

    diff.getRemoved().forEach(uid -> {
      final var entry = this.cachedEgresses.get(uid);
      final var egress = entry.getKey();
      final var resource = entry.getValue();

      futures.add(
        this.egressReconcilerListener.onDeleteEgress(resource, egress)
          // If we succeed to delete the egress we can remove it from the cache.
          .onSuccess(r -> this.cachedEgresses.remove(uid))
          .onFailure(cause -> logFailure("Failed to reconcile [onDeleteEgress] egress", egress, cause, generation))
      );
    });

    diff.getAdded().forEach(uid -> {
      final var entry = egresses.get(uid);
      final var egress = entry.getKey();
      final var resource = entry.getValue();

      futures.add(
        this.egressReconcilerListener.onNewEgress(resource, egress)
          // If we fail to create the egress we can't put it in the cache.
          .onSuccess(r -> this.cachedEgresses.put(egress.getUid(), new SimpleImmutableEntry<>(egress, resource)))
          .onFailure(cause -> logFailure("Failed to reconcile [onNewEgress] egress ", egress, cause, generation))
      );
    });

    diff.getIntersection().forEach(uid -> {
      final var entry = egresses.get(uid);
      final var newEgress = entry.getKey();
      final var newResource = entry.getValue();

      final var cachedEgress = this.cachedEgresses.get(uid);
      final var oldResource = cachedEgress.getValue();
      final var oldEgress = cachedEgress.getKey();

      if (resourceEquals(newResource, oldResource) && egressEquals(newEgress, oldEgress)) {
        logger.info("Nothing changed for egress {} {} {} {}",
          keyValue("id", newEgress.getUid()),
          keyValue("consumerGroup", newEgress.getConsumerGroup()),
          keyValue("destination", newEgress.getDestination()),
          keyValue("contractGeneration", generation)
        );
        return;
      }

      futures.add(
        this.egressReconcilerListener.onUpdateEgress(newResource, newEgress)
          // If we fail to update the egress we can't put it in the cache.
          .onSuccess(r -> this.cachedEgresses.put(newEgress.getUid(), new SimpleImmutableEntry<>(newEgress, newResource)))
          .onFailure(cause -> logFailure("Failed to reconcile [onUpdateEgress] egress ", newEgress, cause, generation))
      );
    });

    // We want to complete the future, once all futures are complete, so use join.
    return CompositeFuture.join(futures).mapEmpty();
  }

  private Future<Void> reconcileIngress(DataPlaneContract.Contract contract) {

    final var generation = contract.getGeneration();

    final Map<String, DataPlaneContract.Resource> newResourcesMap = new HashMap<>(
      contract.getResourcesList()
        .stream()
        .collect(Collectors.toMap(DataPlaneContract.Resource::getUid, Function.identity()))
    );

    final List<Future> futures = new ArrayList<>(newResourcesMap.size() + this.cachedResources.size());

    final var diff = CollectionsUtils.diff(this.cachedResources.keySet(), newResourcesMap.keySet());
    logger.info("Reconcile ingress diff {} {}", keyValue("diff", diff), keyValue("contractGeneration", generation));

    diff.getRemoved().stream()
      .map(this.cachedResources::get)
      .forEach(r -> futures.add(
        this.ingressReconcilerListener.onDeleteIngress(r, r.getIngress())
          .onSuccess(v -> this.cachedResources.remove(r.getUid()))
          .onFailure(cause -> logFailure("Failed to reconcile [onDeleteIngress] ingress", r, cause, generation))
      ));

    diff.getAdded().stream()
      .map(newResourcesMap::get)
      .filter(DataPlaneContract.Resource::hasIngress)
      .forEach(r -> futures.add(
        this.ingressReconcilerListener.onNewIngress(r, r.getIngress())
          .onSuccess(v -> this.cachedResources.put(r.getUid(), r))
          .onFailure(cause -> logFailure("Failed to reconcile [onNewIngress] ingress", r, cause, generation))
      ));

    diff.getIntersection().forEach(uid -> {
      final var oldResource = this.cachedResources.get(uid);
      final var newResource = newResourcesMap.get(uid);
      if (resourceEquals(oldResource, newResource)) {
        return;
      }
      // Add only resources with ingress.
      if (!newResource.hasIngress()) {
        futures.add(
          this.ingressReconcilerListener.onDeleteIngress(oldResource, oldResource.getIngress())
            .onSuccess(r -> this.cachedResources.remove(uid))
            .onFailure(cause -> logFailure("Failed to reconcile [onDeleteIngress] ingress", oldResource, cause, generation))
        );
        return;
      }

      futures.add(
        this.ingressReconcilerListener.onUpdateIngress(newResource, newResource.getIngress())
          .onSuccess(r -> this.cachedResources.put(uid, newResource))
          .onFailure(cause -> logFailure("Failed to reconcile [onUpdateIngress] ingress", newResource, cause, generation))
      );
    });

    // We want to complete the future, once all futures are complete, so use join.
    return CompositeFuture.join(futures).mapEmpty();
  }

  private boolean isReconcilingIngress() {
    return this.ingressReconcilerListener != null;
  }

  private boolean resourceEquals(DataPlaneContract.Resource r1, DataPlaneContract.Resource r2) {
    if (r1 == r2) {
      return true;
    }
    if (r1 == null || r2 == null) {
      return false;
    }
    return Objects.equals(r1.getUid(), r2.getUid())
      && Objects.equals(r1.getTopicsList(), r2.getTopicsList())
      && Objects.equals(r1.getBootstrapServers(), r2.getBootstrapServers())
      && Objects.equals(r1.getIngress(), r2.getIngress())
      && Objects.equals(r1.hasAbsentAuth(), r2.hasAbsentAuth())
      && Objects.equals(r1.hasAuthSecret(), r2.hasAuthSecret())
      && Objects.equals(r1.getAuthSecret(), r2.getAuthSecret())
      && Objects.equals(r1.hasMultiAuthSecret(), r2.hasMultiAuthSecret())
      && Objects.equals(r1.getMultiAuthSecret(), r2.getMultiAuthSecret())
      && Objects.equals(r1.getCloudEventOverrides(), r2.getCloudEventOverrides())
      // In the case of ingress reconcile, do we really care about this one?
      && Objects.equals(r1.getEgressConfig(), r2.getEgressConfig());
  }

  private boolean egressEquals(DataPlaneContract.Egress e1, DataPlaneContract.Egress e2) {
    if (e1 == e2) {
      return true;
    }
    if (e1 == null || e2 == null) {
      return false;
    }
    return Objects.equals(e1.getUid(), e2.getUid())
      && Objects.equals(e1.getConsumerGroup(), e2.getConsumerGroup())
      && Objects.equals(e1.getDestination(), e2.getDestination())
      && Objects.equals(e1.getReplyUrl(), e2.getReplyUrl())
      && Objects.equals(e1.getReplyToOriginalTopic(), e2.getReplyToOriginalTopic())
      && Objects.equals(e1.getEgressConfig(), e2.getEgressConfig())
      && Objects.equals(e1.getFilter(), e2.getFilter())
      && Objects.equals(e1.getKeyType(), e2.getKeyType())
      && Objects.equals(e1.getDialectedFilterList(), e2.getDialectedFilterList());
  }

  private static void logFailure(final String msg, final DataPlaneContract.Egress egress, final Throwable cause, final long generation) {
    logger.error(msg + " {} {} {}",
      keyValue("id", egress.getUid()),
      keyValue("consumerGroup", egress.getConsumerGroup()),
      keyValue("destination", egress.getDestination()),
      keyValue("contractGeneration", generation),
      cause);
  }

  private static void logFailure(final String msg, final DataPlaneContract.Resource resource, final Throwable cause, final long generation) {
    logger.error(msg + " {} {} {}",
      keyValue("id", resource.getUid()),
      keyValue("ingress.path", resource.getIngress().getPath()),
      keyValue("ingress.host", resource.getIngress().getHost()),
      keyValue("topics", resource.getTopicsList()),
      keyValue("contractGeneration", generation),
      cause);
  }
}
