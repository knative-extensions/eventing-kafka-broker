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

import java.util.List;

/**
 * Information about a specific Kafka topic partition
 */
@DataObject(generateConverter = true)
public class PartitionInfo {

  private List<Node> inSyncReplicas;
  private Node leader;
  private int partition;
  private List<Node> replicas;
  private String topic;

  /**
   * Constructor
   */
  public PartitionInfo() {

  }

  /**
   * Constructor
   *
   * @param inSyncReplicas  the subset of the replicas that are in sync
   * @param leader  the node id of the node currently acting as a leader
   * @param partition the partition id
   * @param replicas  the complete set of replicas for this partition
   * @param topic the topic name
   */
  public PartitionInfo(List<Node> inSyncReplicas, Node leader, int partition, List<Node> replicas, String topic) {
    this.inSyncReplicas = inSyncReplicas;
    this.leader = leader;
    this.partition = partition;
    this.replicas = replicas;
    this.topic = topic;
  }

  /**
   * @return  the subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if the leader should fail
   */
  public List<Node> getInSyncReplicas() {
    return this.inSyncReplicas;
  }

  /**
   * Set the subset of the replicas that are in sync
   *
   * @param inSyncReplicas  the subset of the replicas that are in sync
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setInSyncReplicas(List<Node> inSyncReplicas) {
    this.inSyncReplicas = inSyncReplicas;
    return this;
  }

  /**
   * @return  the node id of the node currently acting as a leader for this partition or null if there is no leader
   */
  public Node getLeader() {
    return this.leader;
  }

  /**
   * Set the node id of the node currently acting as a leader
   *
   * @param leader  the node id of the node currently acting as a leader
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setLeader(Node leader) {
    this.leader = leader;
    return this;
  }

  /**
   * @return  the partition id
   */
  public int getPartition() {
    return this.partition;
  }

  /**
   * Set the partition id
   *
   * @param partition the partition id
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  /**
   * @return  the complete set of replicas for this partition regardless of whether they are alive or up-to-date
   */
  public List<Node> getReplicas() {
    return this.replicas;
  }

  /**
   * Set the complete set of replicas for this partition
   *
   * @param replicas  the complete set of replicas for this partition
   * @return  current instance of the class to be fluent
   */
  public PartitionInfo setReplicas(List<Node> replicas) {
    this.replicas = replicas;
    return this;
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
  public PartitionInfo setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  @Override
  public String toString() {

    return "PartitionInfo{" +
            "topic=" + this.topic +
            ", partition=" + this.partition +
            ", inSyncReplicas=" + this.inSyncReplicas +
            ", leader=" + this.leader +
            ", replicas=" + this.replicas +
            "}";
  }
}
