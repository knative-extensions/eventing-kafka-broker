package dev.knative.eventing.control.protocol.impl;

import io.vertx.core.buffer.Buffer;
import java.util.UUID;

class UUIDUtils {

  public static UUID readFromBuffer(Buffer buffer, int from) {
    long lsl = buffer.getLong(from);
    long msl = buffer.getLong(from + 8);
    return new UUID(msl, lsl);
  }

  public static void writeToBuffer(Buffer buffer, UUID uuid, int to) {
    buffer.setLong(to, uuid.getLeastSignificantBits());
    buffer.setLong(to + 8, uuid.getMostSignificantBits());
  }

}
