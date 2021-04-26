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
package dev.knative.eventing.control.protocol.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;

/**
 * This is a noop codec to send ControlMessage through the event bus (works only in local environments)
 * <p>
 * https://github.com/eclipse-vertx/vert.x/issues/3375
 */
public final class ControlMessageImplCodec implements MessageCodec<ControlMessageImpl, ControlMessageImpl> {

  @Override
  public void encodeToWire(Buffer buffer, ControlMessageImpl controlMessage) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ControlMessageImpl decodeFromWire(int i, Buffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ControlMessageImpl transform(
    ControlMessageImpl controlMessage) {
    return controlMessage;
  }

  @Override
  public String name() {
    return getClass().getCanonicalName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }

  /**
   * Register this event codec to the provided event bus
   */
  public static void register(EventBus eventBus) {
    eventBus.registerDefaultCodec(ControlMessageImpl.class, new ControlMessageImplCodec());
  }
}
