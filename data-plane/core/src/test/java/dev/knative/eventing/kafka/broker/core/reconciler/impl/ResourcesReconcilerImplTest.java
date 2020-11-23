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
      .reconcile(List.of(
        baseResource("1-1234").build(),
        baseResource("2-1234").build()
      ))
      .run();
  }

  @Test
  void reconcileIngressAddNewIngressAtSecondStep() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .reconcile(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build(),
        baseResource("2-1234")
          .build()
      ))
      .expect()
      .newIngress("1-1234")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build(),
        baseResource("2-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build()
      ))
      .expect()
      .newIngress("2-1234")
      .then()
      .run();
  }

  @Test
  void reconcileIngressAndRemoveIngressAtSecondStep() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .reconcile(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build()
      ))
      .expect()
      .newIngress("1-1234")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .build()
      ))
      .expect()
      .deletedIngress("1-1234")
      .then()
      .run();
  }

  @Test
  void reconcileIngressAndUpdateIngressAtSecondStep() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .reconcile(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .build()
      ))
      .expect()
      .newIngress("1-1234")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello/world"))
          .build()
      ))
      .expect()
      .updatedIngress("1-1234")
      .then()
      .run();
  }

  @Test
  void reconcileIngressAddUpdateAndRemoveResource() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .reconcile(Collections.emptyList())
      .reconcile(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello/world"))
          .build()
      ))
      .expect()
      .newIngress("1-1234")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .addTopics("hello-2")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello/world"))
          .build()
      ))
      .expect()
      .updatedIngress("1-1234")
      .then()
      .reconcile(Collections.emptyList())
      .expect()
      .deletedIngress("1-1234")
      .then()
      .run();
  }

  @Test
  void reconcileEgressModifyingTheResource() {
    new ResourceReconcilerTestRunner()
      .enableEgressListener()
      .reconcile(List.of(
        baseResource("1-1234").build()
      ))
      .reconcile(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("bbb"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .expect()
      .newEgress("aaa")
      .newEgress("bbb")
      .newEgress("ccc")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(baseEgress("bbb").setFilter(Filter.newBuilder().putAttributes("id", "hello")))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .expect()
      .updatedEgress("bbb")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .expect()
      .deletedEgress("bbb")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .build()
      ))
      .expect()
      .deletedEgress("aaa")
      .deletedEgress("ccc")
      .then()
      .run();
  }

  @Test
  void reconcileEgressModifyingTheGlobalEgressConfig() {
    new ResourceReconcilerTestRunner()
      .enableEgressListener()
      .reconcile(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("bbb"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .expect()
      .newEgress("aaa")
      .newEgress("bbb")
      .newEgress("ccc")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .setEgressConfig(DataPlaneContract.EgressConfig.newBuilder().setRetry(10))
          .addEgresses(egress("aaa"))
          .addEgresses(egress("bbb"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .expect()
      .updatedEgress("aaa")
      .updatedEgress("bbb")
      .updatedEgress("ccc")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("bbb"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .expect()
      .updatedEgress("aaa")
      .updatedEgress("bbb")
      .updatedEgress("ccc")
      .then()
      .run();
  }

  @Test
  void reconcileEgressAddingAndRemovingResource() {
    new ResourceReconcilerTestRunner()
      .enableEgressListener()
      .reconcile(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("bbb"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .expect()
      .newEgress("aaa")
      .newEgress("bbb")
      .newEgress("ccc")
      .then()
      .reconcile(Collections.emptyList())
      .expect()
      .deletedEgress("aaa")
      .deletedEgress("bbb")
      .deletedEgress("ccc")
      .then()
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
