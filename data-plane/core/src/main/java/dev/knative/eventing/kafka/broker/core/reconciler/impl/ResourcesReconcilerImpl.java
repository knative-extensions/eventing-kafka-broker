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
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ResourcesReconcilerImpl implements ResourcesReconciler {

  private static final Logger logger = LoggerFactory.getLogger(ResourcesReconcilerImpl.class);

  private static final int CACHED_RESOURCES_INITIAL_CAPACITY = 32;

  private final IngressReconcilerListener ingressReconcilerListener;
  private final EgressReconcilerListener egressReconcilerListener;

  // Every resource ingress is identified by its resource, so we don't need to store ingress separately
  private final Map<String, DataPlaneContract.Resource> cachedResources;
  // egress uid -> <Egress, Resource>
  private final Map<String, Map.Entry<DataPlaneContract.Egress, DataPlaneContract.Resource>> cachedEgresses;

  private ResourcesReconcilerImpl(
    IngressReconcilerListener ingressReconcilerListener,
    EgressReconcilerListener egressReconcilerListener) {
    if (ingressReconcilerListener == null && egressReconcilerListener == null) {
      throw new NullPointerException("You need to specify at least one listener");
    }
    this.ingressReconcilerListener = ingressReconcilerListener;
    this.egressReconcilerListener = egressReconcilerListener;

    this.cachedResources = new HashMap<>(CACHED_RESOURCES_INITIAL_CAPACITY);
    this.cachedEgresses = new HashMap<>(egressReconcilerListener != null ? CACHED_RESOURCES_INITIAL_CAPACITY : 0);
  }

  @Override
  public Future<Void> reconcile(final Collection<DataPlaneContract.Resource> newResources) {
    if (isReconcilingIngress()) {
      return reconcileIngress(newResources);
    }
    return reconcileEgress(newResources);
  }

  private Future<Void> reconcileEgress(final Collection<DataPlaneContract.Resource> newResources) {

    final var egresses = newResources.stream()
      .filter(r -> r.getEgressesCount() > 0)
      .flatMap(r -> r.getEgressesList()
        .stream()
        // egress uid -> <Egress, Resource>
        .map(e -> new SimpleImmutableEntry<>(e.getUid(), new SimpleImmutableEntry<>(e, r))))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final List<Future> futures = new ArrayList<>(egresses.size() + this.cachedEgresses.size());

    final var diff = CollectionsUtils.diff(this.cachedEgresses.keySet(), egresses.keySet());
    logger.debug("Reconcile egress diff {}", diff);

    diff.getRemoved().forEach(uid -> {
      final var entry = this.cachedEgresses.get(uid);
      final var egress = entry.getKey();
      final var resource = entry.getValue();

      futures.add(
        this.egressReconcilerListener.onDeleteEgress(resource, egress)
          // If we succeed to delete the egress we can remove it from the cache.
          .onSuccess(r -> egresses.remove(uid))
          .onSuccess(r -> this.cachedEgresses.remove(uid))
          .onFailure(cause -> logFailure("Failed to reconcile [onDeleteEgress] egress", egress, cause))
      );
    });

    diff.getAdded().forEach(uid -> {
      final var entry = egresses.get(uid);
      final var egress = entry.getKey();
      final var resource = entry.getValue();

      futures.add(
        this.egressReconcilerListener.onNewEgress(resource, egress)
          // If we fail to create the egress we can't put it in the cache.
          .onFailure(r -> egresses.remove(uid))
          .onSuccess(r -> this.cachedEgresses.put(egress.getUid(), new SimpleImmutableEntry<>(egress, resource)))
          .onFailure(cause -> logFailure("Failed to reconcile [onNewEgress] egress ", egress, cause))
      );
    });

    diff.getIntersection().forEach(uid -> {
      final var entry = egresses.get(uid);
      final var newEgress = entry.getKey();
      final var newResource = entry.getValue();

      final var oldResource = this.cachedResources.get(newResource.getUid());

      if (resourceEquals(newResource, oldResource) && egressEquals(newEgress, this.cachedEgresses.get(uid).getKey())) {
        if (logger.isDebugEnabled()) {
          logger.debug("Nothing changed for egress {} {} {}", newEgress.getUid(), newEgress.getConsumerGroup(),
            newEgress.getDestination());
        }
        return;
      }

      futures.add(
        this.egressReconcilerListener.onUpdateEgress(newResource, newEgress)
          // If we fail to update the egress we can't put it in the cache.
          .onFailure(r -> egresses.remove(uid))
          .onSuccess(r -> this.cachedEgresses.put(newEgress.getUid(), new SimpleImmutableEntry<>(newEgress, newResource)))
          .onFailure(cause -> logFailure("Failed to reconcile [onUpdateEgress] egress ", newEgress, cause))
      );
    });

    // We want to complete the future, once all futures are complete, so use join.
    return CompositeFuture.join(futures)
      .onComplete(r -> {
        this.cachedResources.clear();
        egresses.values()
          .forEach(entry -> {
            final var resource = entry.getValue();
            this.cachedResources.put(resource.getUid(), resource);
          });
      })
      .mapEmpty();
  }

  private Future<Void> reconcileIngress(Collection<DataPlaneContract.Resource> newResources) {

    final Map<String, DataPlaneContract.Resource> newResourcesMap = new HashMap<>(
      newResources
        .stream()
        .collect(Collectors.toMap(DataPlaneContract.Resource::getUid, Function.identity()))
    );

    final List<Future> futures = new ArrayList<>(newResourcesMap.size() + this.cachedResources.size());

    final var diff = CollectionsUtils.diff(this.cachedResources.keySet(), newResourcesMap.keySet());
    logger.debug("Reconcile ingress diff {}", diff);

    diff.getRemoved().stream()
      .map(this.cachedResources::get)
      .forEach(r -> futures.add(
        this.ingressReconcilerListener.onDeleteIngress(r, r.getIngress())
          .onSuccess(v -> this.cachedResources.remove(r.getUid()))
          .onFailure(cause -> logFailure("Failed to reconcile [onDeleteIngress] ingress", r, cause))
      ));

    diff.getAdded().stream()
      .map(newResourcesMap::get)
      .filter(DataPlaneContract.Resource::hasIngress)
      .forEach(r -> futures.add(
        this.ingressReconcilerListener.onNewIngress(r, r.getIngress())
          .onSuccess(v -> this.cachedResources.put(r.getUid(), r))
          .onFailure(cause -> logFailure("Failed to reconcile [onNewIngress] ingress", r, cause))
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
            .onFailure(cause -> logFailure("Failed to reconcile [onDeleteIngress] ingress", oldResource, cause))
        );
        return;
      }

      futures.add(
        this.ingressReconcilerListener.onUpdateIngress(newResource, newResource.getIngress())
          .onSuccess(r -> this.cachedResources.put(uid, newResource))
          .onFailure(cause -> logFailure("Failed to reconcile [onUpdateIngress] ingress", newResource, cause))
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
      && Objects.equals(r1.getAuthSecret(), r2.getAuthSecret())
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
      && Objects.equals(e1.getFilter(), e2.getFilter());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private IngressReconcilerListener ingressReconcilerListener;
    private EgressReconcilerListener egressReconcilerListener;

    public Builder watchIngress(
      IngressReconcilerListener ingressReconcilerListener) {
      Objects.requireNonNull(ingressReconcilerListener);
      this.ingressReconcilerListener = ingressReconcilerListener;
      return this;
    }

    public Builder watchEgress(
      EgressReconcilerListener egressReconcilerListener) {
      Objects.requireNonNull(egressReconcilerListener);
      this.egressReconcilerListener = egressReconcilerListener;
      return this;
    }

    public ResourcesReconcilerImpl build() {
      return new ResourcesReconcilerImpl(ingressReconcilerListener, egressReconcilerListener);
    }
  }

  private static void logFailure(final String msg, final DataPlaneContract.Egress egress, final Throwable cause) {
    MDC.put("id", egress.getUid());
    MDC.put("consumerGroup", egress.getConsumerGroup());
    MDC.put("destination", egress.getDestination());
    logger.error(msg, cause);
    MDC.clear();
  }

  private static void logFailure(final String msg, final DataPlaneContract.Resource resource, final Throwable cause) {
    MDC.put("id", resource.getUid());
    MDC.put("ingress.path", resource.getIngress().getPath());
    MDC.put("topics", resource.getTopicsList().toString());
    logger.error(msg, cause);
    MDC.clear();
  }
}
