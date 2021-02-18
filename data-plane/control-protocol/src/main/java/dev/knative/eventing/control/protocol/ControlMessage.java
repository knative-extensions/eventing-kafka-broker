package dev.knative.eventing.control.protocol;

import io.vertx.core.buffer.Buffer;

public interface ControlMessage extends ControlMessageHeader {

  /**
   * @return the mutable payload buffer
   */
  Buffer payloadBuffer();

  /**
   * @return the message payload
   */
  byte[] payload();

  /**
   * @return the message payload as a Vert.x JSON type
   */
  Object payloadAsJson();

  Buffer toBuffer();

}
