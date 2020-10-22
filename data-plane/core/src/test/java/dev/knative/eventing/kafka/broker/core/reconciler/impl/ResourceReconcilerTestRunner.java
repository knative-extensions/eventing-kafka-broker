package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ResourceReconcilerTestRunner {

  public static class Step {

    private final Collection<DataPlaneContract.Resource> resources;
    private final ResourceReconcilerTestRunner runner;

    private final Set<String> newIngresses = new HashSet<>();
    private final Set<String> updatedIngresses = new HashSet<>();
    private final Set<String> deletedIngresses = new HashSet<>();
    private final Set<String> newEgresses = new HashSet<>();
    private final Set<String> updatedEgresses = new HashSet<>();
    private final Set<String> deletedEgresses = new HashSet<>();

    public Step(Collection<DataPlaneContract.Resource> resources,
                ResourceReconcilerTestRunner runner) {
      this.resources = resources;
      this.runner = runner;
    }

    public Step newIngress(String uid) {
      this.newIngresses.add(uid);
      return this;
    }

    public Step updatedIngress(String uid) {
      this.updatedIngresses.add(uid);
      return this;
    }

    public Step deletedIngress(String uid) {
      this.deletedIngresses.add(uid);
      return this;
    }

    public Step newEgress(String uid) {
      this.newEgresses.add(uid);
      return this;
    }

    public Step updatedEgress(String uid) {
      this.updatedEgresses.add(uid);
      return this;
    }

    public Step deletedEgress(String uid) {
      this.deletedEgresses.add(uid);
      return this;
    }

    public ResourceReconcilerTestRunner done() {
      return runner;
    }
  }

  private final List<Step> steps = new ArrayList<>();
  private boolean enableIngressListener = false;
  private boolean enableEgressListener = false;

  public Step step(Collection<DataPlaneContract.Resource> resources) {
    final var step = new Step(resources, this);
    this.steps.add(step);
    return step;
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

    final var reconcilerBuilder = ResourceReconcilerImpl
      .builder();

    if (ingressListener != null) {
      reconcilerBuilder.watchIngress(ingressListener);
    }
    if (egressListener != null) {
      reconcilerBuilder.watchEgress(egressListener);
    }

    final var reconciler = reconcilerBuilder.build();

    for (int i = 0; i < steps.size(); i++) {
      final var step = steps.get(i);
      reconciler.reconcile(step.resources);

      if (ingressListener != null) {
        assertThat(ingressListener.getNewIngresses())
          .as("New ingresses at step " + i)
          .isEqualTo(step.newIngresses);
        assertThat(ingressListener.getUpdatedIngresses())
          .as("Updated ingresses at step " + i)
          .isEqualTo(step.updatedIngresses);
        assertThat(ingressListener.getDeletedIngresses())
          .as("Deleted ingresses at step " + i)
          .isEqualTo(step.deletedIngresses);

        ingressListener.getNewIngresses().clear();
        ingressListener.getUpdatedIngresses().clear();
        ingressListener.getDeletedIngresses().clear();
      }

      if (egressListener != null) {
        assertThat(egressListener.getNewEgresses())
          .as("New egresses at step " + i)
          .isEqualTo(step.newEgresses);
        assertThat(egressListener.getUpdatedEgresses())
          .as("Updated egresses at step " + i)
          .isEqualTo(step.updatedEgresses);
        assertThat(egressListener.getDeletedEgresses())
          .as("Deleted egresses at step " + i)
          .isEqualTo(step.deletedEgresses);

        egressListener.getNewEgresses().clear();
        egressListener.getUpdatedEgresses().clear();
        egressListener.getDeletedEgresses().clear();
      }
    }
  }

}
