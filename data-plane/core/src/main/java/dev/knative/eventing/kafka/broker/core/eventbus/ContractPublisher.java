package dev.knative.eventing.kafka.broker.core.eventbus;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import java.util.function.Consumer;

/**
 * This object publishes all consumed contracts to the event bus
 */
public class ContractPublisher implements Consumer<DataPlaneContract.Contract> {

  private final static DeliveryOptions DELIVERY_OPTIONS = new DeliveryOptions()
    .setLocalOnly(true);

  private final EventBus eventBus;
  private final String address;

  public ContractPublisher(EventBus eventBus, String address) {
    this.eventBus = eventBus;
    this.address = address;
  }

  @Override
  public void accept(DataPlaneContract.Contract contract) {
    eventBus.publish(address, contract, DELIVERY_OPTIONS);
  }

}
