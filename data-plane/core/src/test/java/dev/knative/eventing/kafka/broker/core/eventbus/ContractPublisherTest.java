package dev.knative.eventing.kafka.broker.core.eventbus;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ContractPublisherTest {

  @Test
  public void publishTest(Vertx vertx, VertxTestContext testContext) {
    ContractMessageCodec.register(vertx.eventBus());

    String address = "aaa";
    DataPlaneContract.Contract expected = CoreObjects.contract();

    vertx.eventBus().localConsumer(address).handler(message -> {
      testContext.verify(() ->
        assertThat(message.body())
          .isEqualTo(expected)
      );
      testContext.completeNow();
    });

    ContractPublisher publisher = new ContractPublisher(vertx.eventBus(), address);
    publisher.accept(expected);
  }

}
