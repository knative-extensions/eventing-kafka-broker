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
package dev.knative.eventing.control.protocol;

import io.vertx.core.buffer.Buffer;

public interface ControlMessage extends ControlMessageHeader {

  byte ACK_OP_CODE = (byte) 0xFF;

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

  /**
   * @return the conversion of this message to {@link Buffer}
   */
  Buffer toBuffer();

}
