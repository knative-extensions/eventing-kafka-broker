/*
 * Copyright 2020 The Knative Authors
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

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ResourcesReconcilerMessageHandlerTest {

  @Test
  public void publishAndReceiveContractTest(final Vertx vertx, final VertxTestContext testContext) {
    ContractMessageCodec.register(vertx.eventBus());

    final DataPlaneContract.Contract expected = CoreObjects.contract();

    ResourcesReconcilerMessageHandler.start(
        vertx.eventBus(),
        contract -> {
          testContext.verify(() -> assertThat(contract).isEqualTo(expected.getResourcesList()));
          testContext.completeNow();
          return Future.succeededFuture();
        });

    final ContractPublisher publisher =
        new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS);
    publisher.accept(expected);
  }
}
