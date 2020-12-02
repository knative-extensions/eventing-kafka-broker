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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ResourcesReconcilerImpl implements ResourcesReconciler {

  private final IngressReconcilerListener ingressReconcilerListener;
  private final EgressReconcilerListener egressReconcilerListener;

  // Every resource ingress is identified by its resource, so we don't need to store ingress separately
  private final Map<String, DataPlaneContract.Resource> cachedResources;
  private final Map<String, DataPlaneContract.Egress> cachedEgresses;

  private ResourcesReconcilerImpl(
    IngressReconcilerListener ingressReconcilerListener,
    EgressReconcilerListener egressReconcilerListener) {
    if (ingressReconcilerListener == null && egressReconcilerListener == null) {
      throw new NullPointerException("You need to specify at least one listener");
    }
    this.ingressReconcilerListener = ingressReconcilerListener;
    this.egressReconcilerListener = egressReconcilerListener;

    this.cachedResources = new HashMap<>();
    this.cachedEgresses = new HashMap<>();
  }

  @Override
  public Future<Void> reconcile(
    Collection<DataPlaneContract.Resource> newResources) {
    final Map<String, DataPlaneContract.Resource> newResourcesMap = new HashMap<>(
      newResources
        .stream()
        .collect(Collectors.toMap(DataPlaneContract.Resource::getUid, Function.identity()))
    );
    final List<Future> futures = new ArrayList<>(newResources.size() * 2);

    final var resourcesIterator = this.cachedResources.entrySet().iterator();
    while (resourcesIterator.hasNext()) {
      final var resourceEntry = resourcesIterator.next();
      final var oldResourceUid = resourceEntry.getKey();
      final var oldResource = resourceEntry.getValue();

      final var newResource = newResourcesMap.remove(oldResourceUid);

      if (newResource != null) {
        // Update to new resource
        resourceEntry.setValue(newResource);

        boolean resourceAreEquals = resourceEquals(oldResource, newResource);

        // Reconcile ingress
        if (isReconcilingIngress() && !resourceAreEquals) {
          if (!oldResource.hasIngress() && newResource.hasIngress()) {
            futures.add(
              this.ingressReconcilerListener.onNewIngress(newResource, newResource.getIngress())
            );
          } else if (oldResource.hasIngress() && !newResource.hasIngress()) {
            futures.add(
              this.ingressReconcilerListener.onDeleteIngress(oldResource, oldResource.getIngress())
            );
          } else if (newResource.hasIngress()) {
            futures.add(
              this.ingressReconcilerListener.onUpdateIngress(newResource, newResource.getIngress())
            );
          }
        }

        // Reconcile egress
        if (isReconcilingEgress()) {
          final var oldEgressesKeys = oldResource
            .getEgressesList()
            .stream()
            .map(DataPlaneContract.Egress::getUid)
            .collect(Collectors.toSet());
          final var newEgresses = newResource
            .getEgressesList()
            .stream()
            .collect(Collectors.toMap(DataPlaneContract.Egress::getUid, Function.identity()));

          final var diff = CollectionsUtils.diff(oldEgressesKeys, newEgresses.keySet());

          // Handle added
          for (String uid : diff.getAdded()) {
            final var newEgress = newEgresses.get(uid);
            this.cachedEgresses.put(uid, newEgress);
            futures.add(
              this.egressReconcilerListener.onNewEgress(newResource, newEgress)
            );
          }

          // Handle removed
          for (String uid : diff.getRemoved()) {
            futures.add(
              this.egressReconcilerListener.onDeleteEgress(oldResource, this.cachedEgresses.remove(uid))
            );
          }

          // Handle intersection
          for (String uid : diff.getIntersection()) {
            final var newEgress = newEgresses.get(uid);
            final var oldEgress = this.cachedEgresses.replace(uid, newEgress);

            if (!egressEquals(newEgress, oldEgress) || !resourceAreEquals) {
              futures.add(
                this.egressReconcilerListener.onUpdateEgress(newResource, newEgress)
              );
            }
          }
        }
      }

      if (newResource == null) {
        // Resource deleted
        resourcesIterator.remove();

        // Reconcile ingress
        if (isReconcilingIngress() && oldResource.hasIngress()) {
          futures.add(
            this.ingressReconcilerListener.onDeleteIngress(oldResource, oldResource.getIngress())
          );
        }

        // Reconcile egress
        if (isReconcilingEgress() && oldResource.getEgressesCount() != 0) {
          for (DataPlaneContract.Egress egress : oldResource.getEgressesList()) {
            this.cachedEgresses.remove(egress.getUid());
            futures.add(
              this.egressReconcilerListener.onDeleteEgress(oldResource, egress)
            );
          }
        }
      }
    }

    // Now newResourcesMap contains only the new resource
    this.cachedResources.putAll(newResourcesMap);
    newResourcesMap.forEach((uid, newResource) -> {
      // Reconcile ingress
      if (isReconcilingIngress() && newResource.hasIngress()) {
        futures.add(
          this.ingressReconcilerListener.onNewIngress(newResource, newResource.getIngress())
        );
      }

      // Reconcile ingress
      if (isReconcilingEgress() && newResource.getEgressesCount() != 0) {
        for (DataPlaneContract.Egress egress : newResource.getEgressesList()) {
          this.cachedEgresses.put(egress.getUid(), egress);
          futures.add(
            this.egressReconcilerListener.onNewEgress(newResource, egress)
          );
        }
      }
    });

    return CompositeFuture.all(futures).mapEmpty();
  }

  private boolean isReconcilingIngress() {
    return this.ingressReconcilerListener != null;
  }

  private boolean isReconcilingEgress() {
    return this.egressReconcilerListener != null;
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
}
