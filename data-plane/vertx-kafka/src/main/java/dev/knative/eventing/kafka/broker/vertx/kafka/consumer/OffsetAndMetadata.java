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

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * Provide additional metadata when an offset is committed
 */
@DataObject
public class OffsetAndMetadata {

  private long offset;
  private String metadata;

  /**
   * Constructor
   */
  public OffsetAndMetadata() {
  }

  /**
   * Constructor
   *
   * @param offset  offset to commit
   * @param metadata  additional metadata with the offset committed
   */
  public OffsetAndMetadata(long offset, String metadata) {
    this.offset = offset;
    this.metadata = metadata;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public OffsetAndMetadata(JsonObject json) {
    this.offset = json.getLong("offset");
    this.metadata = json.getString("metadata");
  }

  /**
   * Constructor (copy)
   *
   * @param that  object to copy
   */
  public OffsetAndMetadata(OffsetAndMetadata that) {
    this.offset = that.offset;
    this.metadata = that.metadata;
  }

  /**
   * @return  offset to commit
   */
  public long getOffset() {
    return this.offset;
  }

  /**
   * Set the offset to commit
   *
   * @param offset  offset to commit
   * @return  current instance of the class to be fluent
   */
  public OffsetAndMetadata setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  /**
   * @return  additional metadata with the offset committed
   */
  public String getMetadata() {
    return this.metadata;
  }

  /**
   * Set additional metadata for the offset committed
   *
   * @param metadata  additional metadata
   * @return  current instance of the class to be fluent
   */
  public OffsetAndMetadata setMetadata(String metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    return new JsonObject().put("offset", this.offset).put("metadata", this.metadata);
  }

  @Override
  public String toString() {

    return "OffsetAndMetadata{" +
      "offset=" + this.offset +
      ", metadata=" + this.metadata +
      "}";
  }
}
