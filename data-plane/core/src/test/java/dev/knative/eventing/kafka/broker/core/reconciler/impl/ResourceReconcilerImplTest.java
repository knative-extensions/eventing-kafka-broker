package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import static dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Resource;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource1Unwrapped;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource2Unwrapped;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.List;
import org.junit.jupiter.api.Test;

class ResourceReconcilerImplTest {

  @Test
  void reconcileIngressAddNewResourcesWithoutIngress() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .step(List.of(resource1Unwrapped(), resource2Unwrapped()))
      .done()
      .run();
  }

  @Test
  void reconcileIngressAddNewIngressAtSecondStep() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .step(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build(),
        baseResource("2-1234")
          .build()
      ))
      .newIngress("1-1234")
      .done()
      .step(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build(),
        baseResource("2-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build()
      ))
      .newIngress("2-1234")
      .done()
      .run();
  }

  @Test
  void reconcileIngressAndRemoveIngressAtSecondStep() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .step(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build()
      ))
      .newIngress("1-1234")
      .done()
      .step(List.of(
        Resource.newBuilder()
          .setUid("1-1234")
          .build()
      ))
      .deletedIngress("1-1234")
      .done()
      .run();
  }

  @Test
  void reconcileIngressAndUpdateIngressAtSecondStep() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .step(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build()
      ))
      .newIngress("1-1234")
      .done()
      .step(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello/world"))
          .build()
      ))
      .updatedIngress("1-1234")
      .done()
      .run();
  }

  private Resource.Builder baseResource(String uid) {
    return Resource.newBuilder().setUid(uid).addTopics("hello.topic");
  }

}
