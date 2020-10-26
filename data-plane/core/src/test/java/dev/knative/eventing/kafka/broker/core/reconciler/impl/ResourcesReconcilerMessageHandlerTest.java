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
  public void publishAndReceiveContractTest(Vertx vertx, VertxTestContext testContext) {
    ContractMessageCodec.register(vertx.eventBus());

    DataPlaneContract.Contract expected = CoreObjects.contract();

    ResourcesReconcilerMessageHandler.start(vertx.eventBus(), contract -> {
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

}
