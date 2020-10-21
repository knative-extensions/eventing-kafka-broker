package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
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

public class ResourceReconcilerImpl implements ResourcesReconciler {

  private final IngressReconcilerListener ingressReconcilerListener;
  private final EgressReconcilerListener egressReconcilerListener;

  // Every resource ingress is identified by its resource, so we don't need to store ingress separately
  private final Map<String, DataPlaneContract.Resource> cachedResources;
  private final Map<String, DataPlaneContract.Egress> cachedEgresses;

  private ResourceReconcilerImpl(
    IngressReconcilerListener ingressReconcilerListener,
    EgressReconcilerListener egressReconcilerListener) {
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

      if (newResource != null && !resourceEquals(oldResource, newResource)) {
        // Resource updated
        resourceEntry.setValue(newResource);

        if (isReconcilingIngress()) {
          if (!oldResource.hasIngress() && newResource.hasIngress()) {
            futures.add(
              this.ingressReconcilerListener.onNewIngress(newResource, newResource.getIngress())
            );
          } else if (!newResource.hasIngress()) {
            futures.add(
              this.ingressReconcilerListener.onDeleteIngress(oldResource, oldResource.getIngress())
            );
          } else {
            futures.add(
              this.ingressReconcilerListener.onUpdateIngress(newResource, newResource.getIngress())
            );
          }
        }
        continue;
      }

      if (newResource == null) {
        // Resource deleted
        resourcesIterator.remove();

        if (isReconcilingIngress() && oldResource.hasIngress()) {
          futures.add(
            this.ingressReconcilerListener.onDeleteIngress(oldResource, oldResource.getIngress())
          );
        }
      }
    }

    // Now newResourcesMap contains only the new resource
    this.cachedResources.putAll(newResourcesMap);
    newResourcesMap.forEach((uid, resource) -> {
      if (isReconcilingIngress() && resource.hasIngress()) {
        futures.add(
          this.ingressReconcilerListener.onNewIngress(resource, resource.getIngress())
        );
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
      && Objects.equals(r1.getEgressConfig(), r2.getEgressConfig());
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

    public ResourceReconcilerImpl build() {
      return new ResourceReconcilerImpl(ingressReconcilerListener, egressReconcilerListener);
    }
  }
}
