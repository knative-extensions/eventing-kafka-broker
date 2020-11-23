package dev.knative.eventing.kafka.broker.core.eventbus;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;

/**
 * This is a noop codec to send the contract on the event bus (works only in local environments)
 * <p>
 * https://github.com/eclipse-vertx/vert.x/issues/3375
 */
public final class ContractMessageCodec
  implements MessageCodec<DataPlaneContract.Contract, DataPlaneContract.Contract> {

  @Override
  public void encodeToWire(Buffer buffer,
                           DataPlaneContract.Contract contract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataPlaneContract.Contract decodeFromWire(int i, Buffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataPlaneContract.Contract transform(
    DataPlaneContract.Contract contract) {
    return contract;
  }

  @Override
  public String name() {
    return getClass().getCanonicalName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }

  public static void register(EventBus eventBus) {
    eventBus.registerDefaultCodec(DataPlaneContract.Contract.class, new ContractMessageCodec());
  }
}
