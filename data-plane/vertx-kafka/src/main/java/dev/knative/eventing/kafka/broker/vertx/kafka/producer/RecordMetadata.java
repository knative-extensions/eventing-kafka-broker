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
package dev.knative.eventing.kafka.broker.vertx.kafka.producer;

import io.vertx.core.json.JsonObject;

/**
 * Metadata related to a Kafka record
 */
public class RecordMetadata {

  private long offset;
  private int partition;
  private long timestamp;
  private String topic;

  /**
   * Constructor
   */
  public RecordMetadata() {

  }

  /**
   * Constructor
   *
   * @param offset    the offset of the record in the topic/partition.
   * @param partition the partition the record was sent to
   * @param timestamp the timestamp of the record in the topic/partition
   * @param topic     the topic the record was appended to
   */
  public RecordMetadata(long offset, int partition, long timestamp, String topic) {
    this.offset = offset;
    this.partition = partition;
    this.timestamp = timestamp;
    this.topic = topic;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json JSON representation
   */
  public RecordMetadata(JsonObject json) {
    this.offset = json.getLong("offset");
    this.partition = json.getInteger("partition");
    this.timestamp = json.getLong("timestamp");
    this.topic = json.getString("topic");
  }

  /**
   * @return the offset of the record in the topic/partition.
   */
  public long getOffset() {
    return this.offset;
  }

  /**
   * Set the offset of the record in the topic/partition.
   *
   * @param offset offset of the record in the topic/partition
   * @return current instance of the class to be fluent
   */
  public RecordMetadata setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  /**
   * @return the partition the record was sent to
   */
  public int getPartition() {
    return this.partition;
  }

  /**
   * Set the partition the record was sent to
   *
   * @param partition the partition the record was sent to
   * @return current instance of the class to be fluent
   */
  public RecordMetadata setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  /**
   * @return the timestamp of the record in the topic/partition
   */
  public long getTimestamp() {
    return this.timestamp;
  }

  /**
   * Set the timestamp of the record in the topic/partition
   *
   * @param timestamp the timestamp of the record in the topic/partition
   * @return current instance of the class to be fluent
   */
  public RecordMetadata setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * @return the topic the record was appended to
   */
  public String getTopic() {
    return this.topic;
  }

  /**
   * Set the topic the record was appended to
   *
   * @param topic the topic the record was appended to
   * @return current instance of the class to be fluent
   */
  public RecordMetadata setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return JSON representation
   */
  public JsonObject toJson() {
    JsonObject jsonObject = new JsonObject();

    jsonObject
      .put("offset", this.offset)
      .put("partition", this.partition)
      .put("timestamp", this.timestamp)
      .put("topic", this.topic);

    return jsonObject;
  }
}
