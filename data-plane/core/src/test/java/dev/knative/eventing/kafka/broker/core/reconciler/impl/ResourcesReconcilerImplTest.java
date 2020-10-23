package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import static dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Egress;
import static dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Filter;
import static dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Resource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ResourcesReconcilerImplTest {

  @Test
  void nullPointerExceptionWhenNoListenerIsProvided() {
    assertThatThrownBy(() -> ResourcesReconcilerImpl.builder().build())
      .isInstanceOf(NullPointerException.class);
  }

  @Test
  void reconcileIngressAddNewResourcesWithoutIngress() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .step(List.of(
        baseResource("1-1234").build(),
        baseResource("2-1234").build()
      ))
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

  @Test
  void reconcileIngressAddUpdateAndRemoveResource() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .step(Collections.emptyList())
      .done()
      .step(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello/world"))
          .build()
      ))
      .newIngress("1-1234")
      .done()
      .step(List.of(
        baseResource("1-1234")
          .addTopics("hello-2")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello/world"))
          .build()
      ))
      .updatedIngress("1-1234")
      .done()
      .step(Collections.emptyList())
      .deletedIngress("1-1234")
      .done()
      .run();
  }

  @Test
  void reconcileEgressModifyingTheResource() {
    new ResourceReconcilerTestRunner()
      .enableEgressListener()
      .step(List.of(
        baseResource("1-1234").build()
      ))
      .done()
      .step(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("bbb"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .newEgress("aaa")
      .newEgress("bbb")
      .newEgress("ccc")
      .done()
      .step(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(baseEgress("bbb").setFilter(Filter.newBuilder().putAttributes("id", "hello")))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .updatedEgress("bbb")
      .done()
      .step(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .deletedEgress("bbb")
      .done()
      .step(List.of(
        baseResource("1-1234")
          .build()
      ))
      .deletedEgress("aaa")
      .deletedEgress("ccc")
      .done()
      .run();
  }

  @Test
  void reconcileEgressAddingAndRemovingResource() {
    new ResourceReconcilerTestRunner()
      .enableEgressListener()
      .step(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("bbb"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .newEgress("aaa")
      .newEgress("bbb")
      .newEgress("ccc")
      .done()
      .step(Collections.emptyList())
      .deletedEgress("aaa")
      .deletedEgress("bbb")
      .deletedEgress("ccc")
      .done()
      .run();
  }

  private Resource.Builder baseResource(String uid) {
    return Resource.newBuilder().setUid(uid).addTopics("hello.topic");
  }

  private Egress.Builder baseEgress(String uid) {
    return Egress.newBuilder().setUid(uid).setConsumerGroup("hello");
  }

  private Egress egress(String uid) {
    return baseEgress(uid).build();
  }

}
