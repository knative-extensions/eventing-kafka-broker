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
package dev.knative.eventing.kafka.broker.vertx.kafka.common;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * Represent information related to a partition for a topic
 */
@DataObject
public class TopicPartition {

  private String topic;
  private int partition;

  /**
   * Constructor
   */
  public TopicPartition() {
  }

  /**
   * Constructor
   *
   * @param topic the topic name
   * @param partition the partition number
   */
  public TopicPartition(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public TopicPartition(JsonObject json) {
    this.topic = json.getString("topic");
    this.partition = json.getInteger("partition");
  }

  /**
   * Constructor (copy)
   *
   * @param that  object to copy
   */
  public TopicPartition(TopicPartition that) {
    this.topic = that.topic;
    this.partition = that.partition;
  }

  /**
   * @return  the topic name
   */
  public String getTopic() {
    return this.topic;
  }

  /**
   * Set the topic name
   *
   * @param topic the topic name
   * @return  current instance of the class to be fluent
   */
  public TopicPartition setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  /**
   * @return  the partition number
   */
  public int getPartition() {
    return this.partition;
  }

  /**
   * Set the partition number
   *
   * @param partition the partition number
   * @return  current instance of the class to be fluent
   */
  public TopicPartition setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    return new JsonObject().put("topic", this.topic).put("partition", this.partition);
  }

  @Override
  public String toString() {

    return "TopicPartition{" +
      "topic=" + this.topic +
      ", partition=" + this.partition +
      "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TopicPartition that = (TopicPartition) o;

    if (partition != that.partition) return false;
    return topic != null ? topic.equals(that.topic) : that.topic == null;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + partition;
    result = 31 * result + (topic != null ? topic.hashCode() : 0);
    return result;
  }
}
