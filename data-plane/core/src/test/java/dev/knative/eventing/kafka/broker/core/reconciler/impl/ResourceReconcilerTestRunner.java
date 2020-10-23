package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ResourceReconcilerTestRunner {

  public static class ReconcileStep {

    private final Collection<DataPlaneContract.Resource> resources;
    private final ResourceReconcilerTestRunner runner;

    private final Set<String> newIngresses = new HashSet<>();
    private final Set<String> updatedIngresses = new HashSet<>();
    private final Set<String> deletedIngresses = new HashSet<>();
    private final Set<String> newEgresses = new HashSet<>();
    private final Set<String> updatedEgresses = new HashSet<>();
    private final Set<String> deletedEgresses = new HashSet<>();

    public ReconcileStep(Collection<DataPlaneContract.Resource> resources,
                         ResourceReconcilerTestRunner runner) {
      this.resources = resources;
      this.runner = runner;
    }

    public ReconcileStep newIngress(String uid) {
      this.newIngresses.add(uid);
      return this;
    }

    public ReconcileStep updatedIngress(String uid) {
      this.updatedIngresses.add(uid);
      return this;
    }

    public ReconcileStep deletedIngress(String uid) {
      this.deletedIngresses.add(uid);
      return this;
    }

    public ReconcileStep newEgress(String uid) {
      this.newEgresses.add(uid);
      return this;
    }

    public ReconcileStep updatedEgress(String uid) {
      this.updatedEgresses.add(uid);
      return this;
    }

    public ReconcileStep deletedEgress(String uid) {
      this.deletedEgresses.add(uid);
      return this;
    }

    public ResourceReconcilerTestRunner then() {
      return runner;
    }
  }

  private final List<ReconcileStep> reconcileSteps = new ArrayList<>();
  private boolean enableIngressListener = false;
  private boolean enableEgressListener = false;

  public ResourceReconcilerTestRunner reconcile(Collection<DataPlaneContract.Resource> resources) {
    final var step = new ReconcileStep(resources, this);
    this.reconcileSteps.add(step);
    return this;
  }

  public ReconcileStep expect() {
    return this.reconcileSteps.get(this.reconcileSteps.size() - 1);
  }

  public ResourceReconcilerTestRunner enableIngressListener() {
    this.enableIngressListener = true;
    return this;
  }

  public ResourceReconcilerTestRunner enableEgressListener() {
    this.enableEgressListener = true;
    return this;
  }

  public void run() {
    final var ingressListener = enableIngressListener ? new IngressReconcilerListenerMock() : null;
    final var egressListener = enableEgressListener ? new EgressReconcilerListenerMock() : null;

    final var reconcilerBuilder = ResourcesReconcilerImpl
      .builder();

    if (ingressListener != null) {
      reconcilerBuilder.watchIngress(ingressListener);
    }
    if (egressListener != null) {
      reconcilerBuilder.watchEgress(egressListener);
    }

    final var reconciler = reconcilerBuilder.build();

    for (int i = 0; i < reconcileSteps.size(); i++) {
      final var step = reconcileSteps.get(i);
      reconciler.reconcile(step.resources);

      if (ingressListener != null) {
        assertThat(ingressListener.getNewIngresses())
          .as("New ingresses at step " + i)
          .containsExactlyInAnyOrderElementsOf(step.newIngresses);
        assertThat(ingressListener.getUpdatedIngresses())
          .as("Updated ingresses at step " + i)
          .containsExactlyInAnyOrderElementsOf(step.updatedIngresses);
        assertThat(ingressListener.getDeletedIngresses())
          .as("Deleted ingresses at step " + i)
          .containsExactlyInAnyOrderElementsOf(step.deletedIngresses);

        ingressListener.getNewIngresses().clear();
        ingressListener.getUpdatedIngresses().clear();
        ingressListener.getDeletedIngresses().clear();
      }

      if (egressListener != null) {
        assertThat(egressListener.getNewEgresses())
          .as("New egresses at step " + i)
          .containsExactlyInAnyOrderElementsOf(step.newEgresses);
        assertThat(egressListener.getUpdatedEgresses())
          .as("Updated egresses at step " + i)
          .containsExactlyInAnyOrderElementsOf(step.updatedEgresses);
        assertThat(egressListener.getDeletedEgresses())
          .as("Deleted egresses at step " + i)
          .containsExactlyInAnyOrderElementsOf(step.deletedEgresses);

        egressListener.getNewEgresses().clear();
        egressListener.getUpdatedEgresses().clear();
        egressListener.getDeletedEgresses().clear();
      }
    }
  }

}
