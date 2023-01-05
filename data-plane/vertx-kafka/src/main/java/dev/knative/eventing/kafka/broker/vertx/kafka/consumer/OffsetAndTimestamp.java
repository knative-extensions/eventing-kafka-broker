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

/*
 * Copied from https://github.com/vert-x3/vertx-kafka-client
 *
 * Copyright 2016 Red Hat Inc.
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
package dev.knative.eventing.kafka.broker.vertx.kafka.consumer;

import io.vertx.core.json.JsonObject;

/**
 * Represent information related to a Offset with timestamp information
 */
public class OffsetAndTimestamp {

  private long offset;
  private long timestamp;

  /**
   * Constructor
   */
  public OffsetAndTimestamp() {
  }

  /**
   * Constructor
   *
   * @param offset the offset
   * @param timestamp the timestamp
   */
  public OffsetAndTimestamp(long offset, long timestamp) {
    this.offset = offset;
    this.timestamp = timestamp;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public OffsetAndTimestamp(JsonObject json) {
    this.offset = json.getLong("offset");
    this.timestamp = json.getLong("timestamp");
  }

  /**
   * Constructor (copy)
   *
   * @param that  object to copy
   */
  public OffsetAndTimestamp(OffsetAndTimestamp that) {
      this.offset = that.offset;
      this.timestamp = that.timestamp;
  }

  /**
   * @return  the offset
   */
  public long getOffset() {
    return this.offset;
  }

  /**
   * Set the offset
   *
   * @param offset the offset
   * @return current instance of the class to be fluent
   */
  public OffsetAndTimestamp setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  /**
   * @return  the timestamp
   */
  public long getTimestamp() {
    return this.timestamp;
  }

  /**
   * Set the timestamp
   *
   * @param timestamp the timestamp
   * @return  current instance of the class to be fluent
   */
  public OffsetAndTimestamp setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    return new JsonObject().put("offset", this.offset).put("timestamp", this.timestamp);
  }

  @Override
  public String toString() {

    return "OffsetAndTimestamp{" +
      "offset=" + this.offset +
      ", timestamp=" + this.timestamp +
      "}";
  }
}
