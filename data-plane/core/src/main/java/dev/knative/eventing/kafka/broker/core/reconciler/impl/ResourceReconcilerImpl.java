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

    this.cachedResources = this.ingressReconcilerListener != null ? new HashMap<>() : null;
    this.cachedEgresses = this.egressReconcilerListener != null ? new HashMap<>() : null;
  }

  @Override
  public Future<Void> reconcile(
    Collection<DataPlaneContract.Resource> newResources) throws Exception {
    final Map<String, DataPlaneContract.Resource> newResourcesMap = newResources
      .stream()
      .collect(Collectors.toMap(DataPlaneContract.Resource::getUid, Function.identity()));
    final List<Future> futures = new ArrayList<>(newResources.size() * 2);

    // diffing previous and new --> remove deleted objects
    final var resourcesIterator = this.cachedResources.entrySet().iterator();
    while (resourcesIterator.hasNext()) {
      final var resourceEntry = resourcesIterator.next();
      final var oldResourceUid = resourceEntry.getKey();

      // check if the old resource has been deleted or updated.
      if (!newResourcesMap.containsKey(oldResourceUid)) {

        // resource deleted or updated, so remove it
        resourcesIterator.remove();

        // undeploy all verticles associated with egresses of the deleted resource.
        for (final var e : resourceEntry.getValue().entrySet()) {
          futures.add(undeployVerticle(oldResourceUid, e.getKey(), e.getValue()));
        }

        continue;
      }

      // resource is there, so check if some egresses have been deleted.
      final var egressesIterator = resourceEntry.getValue().entrySet().iterator();
      while (egressesIterator.hasNext()) {
        final var egressEntry = egressesIterator.next();

        // check if the egress has been deleted or updated.
        if (!resourcesMap.get(oldResourceUid).contains(egressEntry.getKey())) {

          // egress deleted or updated, so remove it
          egressesIterator.remove();

          // undeploy verticle associated with the deleted egress.
          futures.add(undeployVerticle(
            oldResourceUid,
            egressEntry.getKey(),
            egressEntry.getValue()
          ));
        }

      }
    }

    // add all new objects.
    for (final var entry : newResources.entrySet()) {
      final var resource = entry.getKey();
      for (final var egress : entry.getValue()) {
        futures.add(addResource(resource, egress));
      }
    }

    return CompositeFuture.all(futures).mapEmpty();
  }

  private boolean isReconcilingIngress() {
    return this.cachedResources != null;
  }

  private boolean isReconcilingEgress() {
    return this.cachedEgresses != null;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private IngressReconcilerListener ingressReconcilerListener;
    private EgressReconcilerListener egressReconcilerListener;

    public Builder watchIngress(
      IngressReconcilerListener ingressReconcilerListener) {
      this.ingressReconcilerListener = ingressReconcilerListener;
      return this;
    }

    public Builder watchEgress(
      EgressReconcilerListener egressReconcilerListener) {
      this.egressReconcilerListener = egressReconcilerListener;
      return this;
    }

    public ResourceReconcilerImpl build() {
      return new ResourceReconcilerImpl(ingressReconcilerListener, egressReconcilerListener);
    }
  }
}
