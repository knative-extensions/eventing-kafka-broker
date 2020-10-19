package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import io.vertx.core.Future;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
    // Logic
    return null;
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
