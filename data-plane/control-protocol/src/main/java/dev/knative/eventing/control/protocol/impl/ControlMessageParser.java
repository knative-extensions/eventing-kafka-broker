package dev.knative.eventing.control.protocol.impl;

import dev.knative.eventing.control.protocol.ControlMessage;
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
      pendingBuffer.appendBuffer(inputBuffer);
    }

    if (pendingBuilder == null) {
      pendingBuilder = new ControlMessageImpl.Builder();
    }

    // Parse the header
    if (!headerParsed) {
      if (pendingBuffer.length() >= MessageConstants.MESSAGE_HEADER_LENGTH) {
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
        pendingBuffer = pendingBuffer.getBuffer(24, pendingBuffer.length());

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
      if (pendingBuffer.length() > payloadLength) {
        pendingBuffer = pendingBuffer.getBuffer(0, payloadLength);
        remainingBuffer = pendingBuffer.getBuffer(payloadLength, pendingBuffer.length());
      }

      // Set the payload
      pendingBuilder.setPayload(pendingBuffer);

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
