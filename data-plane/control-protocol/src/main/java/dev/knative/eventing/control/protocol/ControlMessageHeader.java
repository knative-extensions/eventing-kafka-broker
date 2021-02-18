package dev.knative.eventing.control.protocol;

import java.util.UUID;

public interface ControlMessageHeader {

  /**
   * @return the message op code
   */
  short opCode();

  /**
   * @return the message UUID
   */
  UUID uuid();

  /**
   * @return the message length
   */
  int length();

}
