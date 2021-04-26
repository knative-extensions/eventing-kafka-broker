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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import java.util.Objects;
import java.util.UUID;

public class ControlMessageImpl implements ControlMessage {

  static class Builder {
    private byte version;
    private byte flags;
    private Byte opCode;
    private UUID uuid;
    private int length;
    private Buffer payload;

    public Builder() {
      this.version = ControlMessageHeader.DEFAULT_VERSION;
      this.flags = 0;
      this.length = 0;
    }

    public Builder setVersion(byte version) {
      this.version = version;
      return this;
    }

    public Builder setFlags(byte flags) {
      this.flags = flags;
      return this;
    }

    public Builder setOpCode(byte opCode) {
      this.opCode = opCode;
      return this;
    }

    public Builder setUuid(UUID uuid) {
      this.uuid = uuid;
      return this;
    }

    public Builder setLength(int length) {
      this.length = length;
      return this;
    }

    public Builder setPayload(Buffer payload) {
      this.payload = payload;
      return this;
    }

    public Builder setLengthAndPayload(Buffer payload) {
      this.length = payload.length();
      this.payload = payload;
      return this;
    }

    public ControlMessage build() {
      Objects.requireNonNull(opCode);
      Objects.requireNonNull(uuid);
      return new ControlMessageImpl(version, flags, opCode, uuid, length, payload);
    }
  }

  private final byte version;
  private final byte flags;
  private final byte opCode;
  private final UUID uuid;
  private final int length;
  private final Buffer payload;

  ControlMessageImpl(byte version, byte flags, byte opCode, UUID uuid, int length,
                     Buffer payload) {
    this.version = version;
    this.flags = flags;
    this.opCode = opCode;
    this.uuid = uuid;
    this.length = length;
    this.payload = payload;
  }

  public byte version() {
    return this.version;
  }

  public byte flags() {
    return this.flags;
  }

  @Override
  public byte opCode() {
    return this.opCode;
  }

  @Override
  public UUID uuid() {
    return this.uuid;
  }

  @Override
  public int length() {
    return this.length;
  }

  @Override
  public Buffer payloadBuffer() {
    return this.payload;
  }

  @Override
  public byte[] payload() {
    return this.payload.getBytes();
  }

  @Override
  public Object payloadAsJson() {
    return Json.decodeValue(this.payload);
  }

  @Override
  public Buffer toBuffer() {
    Buffer buffer = Buffer.buffer(
      ControlMessageHeader.MESSAGE_HEADER_LENGTH + ((this.payload != null) ? this.payload.length() : 0));
    buffer.setByte(0, this.version);
    buffer.setByte(1, this.flags);
    buffer.setByte(2, (byte) 0x00);
    buffer.setByte(3, this.opCode);
    UUIDUtils.writeToBuffer(buffer, this.uuid, 4);
    buffer.setInt(20, this.length);
    if (this.payload != null) {
      buffer.setBuffer(24, this.payload);
    }
    return buffer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ControlMessageImpl that = (ControlMessageImpl) o;
    return version == that.version && flags == that.flags && opCode == that.opCode &&
      length == that.length && Objects.equals(uuid, that.uuid) &&
      Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, flags, opCode, uuid, length, payload);
  }

  @Override
  public String toString() {
    return "ControlMessageImpl{" +
      "  version=" + version +
      "\n" +
      "  , flags=" + flags +
      "\n" +
      "  , opCode=" + opCode +
      "\n" +
      "  , uuid=" + uuid +
      "\n" +
      "  , length=" + length +
      "\n" +
      "  , payload=" + payload +
      "\n" +
      '}';
  }
}
