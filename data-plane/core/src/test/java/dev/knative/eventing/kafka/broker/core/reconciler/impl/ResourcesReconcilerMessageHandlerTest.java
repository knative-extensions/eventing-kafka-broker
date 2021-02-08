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
import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ResourcesReconcilerMessageHandlerTest {

  @Test
  public void publishAndReceiveContractTest(Vertx vertx, VertxTestContext testContext) {
    ContractMessageCodec.register(vertx.eventBus());

    DataPlaneContract.Contract expected = CoreObjects.contract();

    ResourcesReconcilerMessageHandler.start(vertx, contract -> {
      testContext.verify(() ->
        assertThat(contract)
          .isEqualTo(expected.getResourcesList())
      );
      testContext.completeNow();
      return Future.succeededFuture();
    });

    ContractPublisher publisher = new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);
    publisher.accept(expected);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldReconcileOnConcurrentEnqueue() {

    final var reconcileCallsCounter = new AtomicInteger();

    final var duringReconcileLatch = new CountDownLatch(1);
    final var newContractSetLatch = new CountDownLatch(1);

    final var handler = new ResourcesReconcilerMessageHandler(resources -> {
      reconcileCallsCounter.incrementAndGet();
      try {
        duringReconcileLatch.countDown();
        newContractSetLatch.await();
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
      return Future.succeededFuture();
    });

    final var contractGenerator = new AtomicInteger();

    final Runnable f = () -> {
      final Message<Object> message = mock(Message.class);
      when(message.body()).thenReturn(DataPlaneContract.Contract
        .newBuilder()
        .setGeneration(contractGenerator.getAndIncrement())
        .build());
      handler.handle(message);
    };

    final var waitFirst = new CountDownLatch(1);
    Executors.newSingleThreadExecutor().submit(() -> {
      waitFirst.countDown();
      f.run();
    });

    Executors.newSingleThreadExecutor().submit(() -> {
      try {
        waitFirst.await();
        duringReconcileLatch.await();
        f.run();
        newContractSetLatch.countDown();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });

    await()
      .untilAsserted(() -> assertThat(reconcileCallsCounter.get()).isEqualTo(2));
  }
}
