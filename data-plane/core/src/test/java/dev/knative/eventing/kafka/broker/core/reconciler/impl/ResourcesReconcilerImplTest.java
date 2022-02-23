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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Egress;
import static dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Filter;
import static dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Resource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResourcesReconcilerImplTest {

  @Test
  void nullPointerExceptionWhenNoListenerIsProvided() {
    assertThatThrownBy(() -> ResourcesReconciler.builder().build())
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
  void reconcileIngressAndAddAuthConfigAtSecondStepAndUpdateAuthConfigAtThirdStep() {
    final var uuid = UUID.randomUUID().toString();
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
          .setAuthSecret(DataPlaneContract.Reference.newBuilder()
            .setName("n1")
            .setNamespace("ns1")
            .setUuid(uuid)
            .setVersion("1"))
          .build()
      ))
      .expect()
      .updatedIngress("1-1234")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
          .setAuthSecret(DataPlaneContract.Reference.newBuilder()
            .setName("n1")
            .setNamespace("ns1")
            .setUuid(uuid)
            .setVersion("2"))
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
  public void reconcileEgressMultipleResourcesMultipleSteps() {

    final var resources1 = List.of(
      baseResource("1")
        .addEgresses(egress("1"))
        .addEgresses(egress("2"))
        .addEgresses(egress("3"))
        .setBootstrapServers("b1,b2")
        .build(),
      baseResource("2")
        .addEgresses(egress("4"))
        .addEgresses(egress("5"))
        .addEgresses(egress("6"))
        .setBootstrapServers("b1,b2")
        .build(),
      baseResource("3")
        .addEgresses(egress("7"))
        .addEgresses(egress("8"))
        .addEgresses(egress("9"))
        .setBootstrapServers("b1,b2")
        .build()
    );

    final var resources2 = List.of(
      baseResource("1")
        .addEgresses(egress("1"))
        .addEgresses(baseEgress("2").setDestination("http://localhost:9091"))
        .addEgresses(egress("3"))
        .setBootstrapServers("b1,b2")
        .build(),
      baseResource("2")
        .addEgresses(egress("4"))
        .addEgresses(egress("5"))
        .addEgresses(egress("6"))
        .setBootstrapServers("b1,b2")
        .build(),
      baseResource("3")
        .addEgresses(egress("7"))
        .addEgresses(egress("8"))
        .setBootstrapServers("b1,b2")
        .build()
    );

    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .reconcile(resources1)
      .expect()
      .newEgress("1")
      .newEgress("2")
      .newEgress("3")
      .newEgress("4")
      .newEgress("5")
      .newEgress("6")
      .newEgress("7")
      .newEgress("8")
      .newEgress("9")
      .then()
      .reconcile(resources2)
      .expect()
      .updatedEgress("2")
      .deletedEgress("9")
      .then()
      .reconcile(resources1)
      .expect()
      .updatedEgress("2")
      .newEgress("9")
      .then()
      .reconcile(resources2)
      .expect()
      .updatedEgress("2")
      .deletedEgress("9")
      .then()
      .reconcile(resources2)
      .expect()
      .then()
      .reconcile(resources1)
      .expect()
      .updatedEgress("2")
      .newEgress("9")
      .then()
      .reconcile(resources1)
      .expect()
      .updatedEgress("2")
      .deletedEgress("9")
      .then()
      .run();
  }

  @Test
  public void reconcileFailedOnNewEgress() {
    new ResourceReconcilerTestRunner()
      .enableEgressListener(new EgressReconcilerListenerMock(
        Future.failedFuture(new RuntimeException()),
        Future.succeededFuture(),
        Future.succeededFuture()
      ))
      .reconcile(List.of(
        baseResource("1")
          .addEgresses(egress("1"))
          .build()
      ))
      .expect()
      .newEgress("1")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .reconcile(List.of(
        baseResource("1")
          .addEgresses(egress("1"))
          .addEgresses(egress("2"))
          .build()
      ))
      .expect()
      .newEgress("1")
      .newEgress("2")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .run();
  }

  @Test
  public void reconcileFailedOnUpdateEgress() {
    new ResourceReconcilerTestRunner()
      .enableEgressListener(new EgressReconcilerListenerMock(
        Future.succeededFuture(),
        Future.failedFuture(new RuntimeException()),
        Future.succeededFuture()
      ))
      .reconcile(List.of(
        baseResource("1")
          .addEgresses(egress("1"))
          .build()
      ))
      .expect()
      .newEgress("1")
      .then()
      .reconcile(List.of(
        baseResource("1")
          .addEgresses(baseEgress("1").setConsumerGroup("foo"))
          .addEgresses(egress("2"))
          .build()
      ))
      .expect()
      .updatedEgress("1")
      .newEgress("2")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .reconcile(List.of(
        baseResource("1")
          .addEgresses(baseEgress("1").setConsumerGroup("foo"))
          .addEgresses(egress("2"))
          .build()
      ))
      .expect()
      .updatedEgress("1")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .run();
  }

  @Test
  public void reconcileFailedOnDeleteEgress() {
    new ResourceReconcilerTestRunner()
      .enableEgressListener(new EgressReconcilerListenerMock(
        Future.succeededFuture(),
        Future.succeededFuture(),
        Future.failedFuture(new RuntimeException())
      ))
      .reconcile(List.of(
        baseResource("1")
          .addEgresses(egress("1"))
          .addEgresses(egress("2"))
          .build()
      ))
      .expect()
      .newEgress("1")
      .newEgress("2")
      .then()
      .reconcile(List.of(
        baseResource("1")
          .addEgresses(baseEgress("1").setConsumerGroup("foo"))
          .addEgresses(egress("3"))
          .build()
      ))
      .expect()
      .updatedEgress("1")
      .deletedEgress("2")
      .newEgress("3")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .reconcile(List.of(
        baseResource("1")
          .addEgresses(baseEgress("1").setConsumerGroup("foo"))
          .addEgresses(baseEgress("3").setDestination("http://localhost"))
          .build()
      ))
      .expect()
      .deletedEgress("2")
      .updatedEgress("3")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .run();
  }

  @Test
  public void reconcileFailedOnUpdateIngress() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener(new IngressReconcilerListenerMock(
        Future.succeededFuture(),
        Future.failedFuture(new RuntimeException()),
        Future.succeededFuture()
      ))
      .reconcile(List.of(
        baseResource("1")
          .setIngress(DataPlaneContract.Ingress
            .newBuilder()
            .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
            .setPath("/hello1/hello2")
            .build())
          .setBootstrapServers("b1,b2")
          .build()
      ))
      .expect()
      .newIngress("1")
      .then()
      .reconcile(List.of(
        baseResource("1")
          .setIngress(DataPlaneContract.Ingress
            .newBuilder()
            .setContentMode(DataPlaneContract.ContentMode.BINARY)
            .setPath("/hello1/hello2")
            .build())
          .setBootstrapServers("b1,b2")
          .build()
      ))
      .expect()
      .updatedIngress("1")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .reconcile(List.of(
        baseResource("1")
          .setIngress(DataPlaneContract.Ingress
            .newBuilder()
            .setContentMode(DataPlaneContract.ContentMode.BINARY)
            .setPath("/hello1/hello2")
            .build())
          .setBootstrapServers("b1,b2")
          .build()
      ))
      .expect()
      .updatedIngress("1")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .run();
  }

  @Test
  public void reconcileFailedOnDeleteIngress() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener(new IngressReconcilerListenerMock(
        Future.succeededFuture(),
        Future.succeededFuture(),
        Future.failedFuture(new RuntimeException())
      ))
      .reconcile(List.of(
        baseResource("1")
          .setIngress(DataPlaneContract.Ingress
            .newBuilder()
            .setContentMode(DataPlaneContract.ContentMode.BINARY)
            .setPath("/hello1/hello")
            .build())
          .setBootstrapServers("b1,b2")
          .build()
      ))
      .expect()
      .newIngress("1")
      .then()
      .reconcile(List.of())
      .expect()
      .deletedIngress("1")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .reconcile(List.of())
      .expect()
      .deletedIngress("1")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .run();
  }

  @Test
  public void reconcileIngressMultipleResourcesMultipleSteps() {

    final var resources1 = List.of(
      baseResource("1")
        .setIngress(DataPlaneContract.Ingress
          .newBuilder()
          .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
          .setPath("/hello1/hello")
          .build())
        .setBootstrapServers("b1,b2")
        .build(),
      baseResource("2")
        .setIngress(DataPlaneContract.Ingress
          .newBuilder()
          .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
          .setPath("/hello2/hello")
          .build())
        .setBootstrapServers("b1,b2")
        .build(),
      baseResource("3")
        .setIngress(DataPlaneContract.Ingress
          .newBuilder()
          .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
          .setPath("/hello3/hello")
          .build())
        .setBootstrapServers("b1,b2")
        .build()
    );

    final var resources2 = List.of(
      baseResource("1")
        .setIngress(DataPlaneContract.Ingress
          .newBuilder()
          .setContentMode(DataPlaneContract.ContentMode.BINARY)
          .setPath("/hello/hello")
          .build())
        .setBootstrapServers("b1,b2")
        .build(),
      baseResource("2")
        .setIngress(DataPlaneContract.Ingress
          .newBuilder()
          .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
          .setPath("/hello2/hello")
          .build())
        .setBootstrapServers("b1,b2")
        .build()
    );

    final var resources3 = List.of(
      baseResource("1")
        .setIngress(DataPlaneContract.Ingress
          .newBuilder()
          .setContentMode(DataPlaneContract.ContentMode.BINARY)
          .setPath("/hello1/hello")
          .build())
        .setBootstrapServers("b1,b2")
        .build(),
      baseResource("2")
        .setBootstrapServers("b1,b2")
        .build()
    );

    new ResourceReconcilerTestRunner()
      .enableIngressListener()
      .reconcile(resources1)
      .expect()
      .newIngress("1")
      .newIngress("2")
      .newIngress("3")
      .then()
      .reconcile(resources2)
      .expect()
      .updatedIngress("1")
      .deletedIngress("3")
      .then()
      .reconcile(resources1)
      .expect()
      .updatedIngress("1")
      .newIngress("3")
      .then()
      .reconcile(resources2)
      .expect()
      .updatedIngress("1")
      .deletedIngress("3")
      .then()
      .reconcile(resources3)
      .expect()
      .deletedIngress("2")
      .updatedIngress("1")
      .then()
      .reconcile(resources2)
      .expect()
      .newIngress("2")
      .updatedIngress("1")
      .then()
      .reconcile(resources1)
      .expect()
      .updatedIngress("1")
      .newIngress("3")
      .then()
      .reconcile(resources1)
      .expect()
      .then()
      .reconcile(resources3)
      .expect()
      .updatedIngress("1")
      .deletedIngress("2")
      .deletedIngress("3")
      .then()
      .run();
  }

  @Test
  public void reconcileFailedOnNewIngress() {
    new ResourceReconcilerTestRunner()
      .enableIngressListener(new IngressReconcilerListenerMock(
        Future.failedFuture(new RuntimeException()),
        Future.succeededFuture(),
        Future.succeededFuture()
      ))
      .reconcile(List.of(
        baseResource("1")
          .setIngress(DataPlaneContract.Ingress
            .newBuilder()
            .setContentMode(DataPlaneContract.ContentMode.BINARY)
            .setPath("/hello1/hello")
            .build())
          .setBootstrapServers("b1,b2")
          .build(),
        baseResource("2")
          .setBootstrapServers("b1,b2")
          .build()
      ))
      .expect()
      .newIngress("1")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
      .then()
      .reconcile(List.of(
        baseResource("1")
          .setIngress(DataPlaneContract.Ingress
            .newBuilder()
            .setContentMode(DataPlaneContract.ContentMode.BINARY)
            .setPath("/hello1/hello")
            .build())
          .setBootstrapServers("b1,b2")
          .build(),
        baseResource("2")
          .setBootstrapServers("b1,b2")
          .setIngress(DataPlaneContract.Ingress
            .newBuilder()
            .setContentMode(DataPlaneContract.ContentMode.BINARY)
            .setPath("/hello2/hello")
            .build())
          .setBootstrapServers("b1,b2")
          .build()
      ))
      .expect()
      .newIngress("1")
      .newIngress("2")
      .returnsFuture(Future.failedFuture(new RuntimeException()))
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
  void reconcileEgressModifyingTheResourceWithDialectedFilters() {
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
          .addEgresses(egress("ddd"))
          .build()
      ))
      .expect()
      .newEgress("aaa")
      .newEgress("bbb")
      .newEgress("ccc")
      .newEgress("ddd")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(baseEgress("bbb").addDialectedFilter(DataPlaneContract.DialectedFilter.newBuilder().setExact(
            DataPlaneContract.Exact.newBuilder().putAttributes("id", "hello").build()
          )))
          .addEgresses(baseEgress("ccc").addDialectedFilter(DataPlaneContract.DialectedFilter.newBuilder().setPrefix(
            DataPlaneContract.Prefix.newBuilder().putAttributes("source", "dev.knative").build()
          )))
          .addEgresses(baseEgress("ddd").addDialectedFilter(DataPlaneContract.DialectedFilter.newBuilder().getAllBuilder()
            .addFiltersBuilder().setPrefix(DataPlaneContract.Prefix.newBuilder().putAttributes("source", "dev.knative").build()).build()
          ))
          .build()
      ))
      .expect()
      .updatedEgress("bbb")
      .updatedEgress("ccc")
      .updatedEgress("ddd")
      .then()
      .reconcile(List.of(
        baseResource("1-1234")
          .addEgresses(egress("aaa"))
          .addEgresses(egress("ccc"))
          .build()
      ))
      .expect()
      .deletedEgress("bbb")
      .deletedEgress("ddd")
      .updatedEgress("ccc")
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
  void reconcileEgressModifyingAuthConfig() {
    final var uuid = UUID.randomUUID().toString();
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
          .setAuthSecret(DataPlaneContract.Reference.newBuilder()
            .setName("n1")
            .setNamespace("ns1")
            .setUuid(uuid)
            .setVersion("1"))
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
          .setAuthSecret(DataPlaneContract.Reference.newBuilder()
            .setName("n1")
            .setNamespace("ns1")
            .setUuid(uuid)
            .setVersion("2"))
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
