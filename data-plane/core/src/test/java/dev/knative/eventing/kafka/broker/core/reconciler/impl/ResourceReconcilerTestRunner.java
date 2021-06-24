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
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ResourceReconcilerTestRunner {

  public static class ReconcileStep {

    private final Collection<DataPlaneContract.Resource> resources;
    private final ResourceReconcilerTestRunner runner;

    private final List<String> newIngresses = new ArrayList<>();
    private final List<String> updatedIngresses = new ArrayList<>();
    private final List<String> deletedIngresses = new ArrayList<>();
    private final List<String> newEgresses = new ArrayList<>();
    private final List<String> updatedEgresses = new ArrayList<>();
    private final List<String> deletedEgresses = new ArrayList<>();
    private Future<Void> future = Future.succeededFuture();

    public ReconcileStep(final Collection<DataPlaneContract.Resource> resources,
                         final ResourceReconcilerTestRunner runner) {
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

    public ReconcileStep returnsFuture(final Future<Void> f) {
      this.future = f;
      return this;
    }

    public ResourceReconcilerTestRunner then() {
      return runner;
    }
  }

  private final List<ReconcileStep> reconcileSteps = new ArrayList<>();
  private IngressReconcilerListenerMock ingressReconcilerListener;
  private EgressReconcilerListenerMock egressReconcilerListener;

  public ResourceReconcilerTestRunner reconcile(Collection<DataPlaneContract.Resource> resources) {
    final var step = new ReconcileStep(resources, this);
    this.reconcileSteps.add(step);
    return this;
  }

  public ReconcileStep expect() {
    return this.reconcileSteps.get(this.reconcileSteps.size() - 1);
  }

  public ResourceReconcilerTestRunner enableIngressListener(final IngressReconcilerListenerMock mock) {
    assertThat(this.egressReconcilerListener)
      .as("One of ingressListener or egressListener is expected, got both")
      .isNull();
    this.ingressReconcilerListener = mock;
    return this;
  }

  public ResourceReconcilerTestRunner enableIngressListener() {
    return enableIngressListener(new IngressReconcilerListenerMock());
  }

  public ResourceReconcilerTestRunner enableEgressListener(final EgressReconcilerListenerMock mock) {
    assertThat(this.ingressReconcilerListener)
      .as("One of ingressListener or egressListener is expected, got both")
      .isNull();
    this.egressReconcilerListener = mock;
    return this;
  }

  public ResourceReconcilerTestRunner enableEgressListener() {
    return enableEgressListener(new EgressReconcilerListenerMock());
  }

  public void run() {
    final var ingressListener = this.ingressReconcilerListener;
    final var egressListener = this.egressReconcilerListener;

    final var reconcilerBuilder = ResourcesReconciler
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
      assertThat(reconciler.reconcile(step.resources).succeeded())
        .as("Step " + i)
        .isEqualTo(step.future.succeeded());

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
