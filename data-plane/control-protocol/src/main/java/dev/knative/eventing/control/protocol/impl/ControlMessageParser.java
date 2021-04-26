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

import dev.knative.eventing.control.protocol.ControlMessage;
import dev.knative.eventing.control.protocol.ControlMessageHeader;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

public class ControlMessageParser implements Handler<Buffer> {

  private final Handler<ControlMessage> controlMessageHandler;

  // Parsing state
  private Buffer pendingBuffer;
  private ControlMessageImpl.Builder pendingBuilder;
  private boolean headerParsed;
  private int payloadLength;

  public ControlMessageParser(Handler<ControlMessage> controlMessageHandler) {
    this.controlMessageHandler = controlMessageHandler;
  }

  @Override
  public void handle(Buffer inputBuffer) {
    if (pendingBuffer == null) {
      pendingBuffer = inputBuffer;
    } else {
      // We need to copy because we don't know if the original buffer was bounded or not
      // nor we want to modfy the original data
      pendingBuffer = pendingBuffer.copy();
      pendingBuffer.appendBuffer(inputBuffer);
    }

    if (pendingBuilder == null) {
      pendingBuilder = new ControlMessageImpl.Builder();
    }

    // Parse the header
    if (!headerParsed) {
      if (pendingBuffer.length() >= ControlMessageHeader.MESSAGE_HEADER_LENGTH) {
        // We're ready to read the message header
        pendingBuilder
          .setVersion(pendingBuffer.getByte(0))
          .setFlags(pendingBuffer.getByte(1))
          .setOpCode(pendingBuffer.getByte(3))
          .setUuid(UUIDUtils.readFromBuffer(pendingBuffer, 4));

        payloadLength = pendingBuffer.getInt(20);
        pendingBuilder
          .setLength(payloadLength);

        headerParsed = true;
        pendingBuffer = pendingBuffer.slice(24, pendingBuffer.length());

      } else {
        // We can't do anything else!
        return;
      }
    }

    // Header parsed, the pending buffer now contains a slice beginning from the first byte
    // of the payload. Now let's check if we can "cut" the payload
    if (pendingBuffer.length() >= payloadLength) {
      // Save any additional bytes in the buffer
      Buffer remainingBuffer = null;
      Buffer payloadBuffer = null;
      if (pendingBuffer.length() > payloadLength) {
        payloadBuffer = pendingBuffer.slice(0, payloadLength);
        remainingBuffer = pendingBuffer.slice(payloadLength, pendingBuffer.length());
      } else {
        payloadBuffer = pendingBuffer;
      }

      if (payloadLength > 0) {
        // Set the payload
        pendingBuilder.setPayload(payloadBuffer);
      }

      // Let's emit the message
      controlMessageHandler.handle(pendingBuilder.build());

      pendingBuilder = null;
      pendingBuffer = null;
      headerParsed = false;
      payloadLength = -1;

      if (remainingBuffer != null) {
        handle(remainingBuffer);
      }
    }
  }

}
