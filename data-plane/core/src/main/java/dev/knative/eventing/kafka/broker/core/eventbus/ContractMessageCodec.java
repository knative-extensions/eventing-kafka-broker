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
package dev.knative.eventing.kafka.broker.core.eventbus;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;

/**
 * This is a noop codec to send the contract on the event bus (works only in local environments)
 *
 * <p>https://github.com/eclipse-vertx/vert.x/issues/3375
 */
public final class ContractMessageCodec
    implements MessageCodec<DataPlaneContract.Contract, DataPlaneContract.Contract> {

  @Override
  public void encodeToWire(final Buffer buffer, final DataPlaneContract.Contract contract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataPlaneContract.Contract decodeFromWire(final int i, final Buffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataPlaneContract.Contract transform(final DataPlaneContract.Contract contract) {
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

  public static void register(final EventBus eventBus) {
    eventBus.registerDefaultCodec(DataPlaneContract.Contract.class, new ContractMessageCodec());
  }
}
