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
 * A class containing leadership, replicas and ISR information for a topic partition.
 */
@DataObject(generateConverter = true)
public class TopicPartitionInfo {

  private List<Node> isr;
  private Node leader;
  private int partition;
  private List<Node> replicas;

  /**
   * Constructor
   */
  public TopicPartitionInfo() {

  }

  /**
   * Constructor
   *
   * @param isr  the subset of the replicas that are in sync
   * @param leader  the node id of the node currently acting as a leader
   * @param partition the partition id
   * @param replicas  the complete set of replicas for this partition
   */
  public TopicPartitionInfo(List<Node> isr, Node leader, int partition, List<Node> replicas) {
    this.isr = isr;
    this.leader = leader;
    this.partition = partition;
    this.replicas = replicas;
  }

  /**
   * @return  the subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if the leader should fail
   */
  public List<Node> getIsr() {
    return this.isr;
  }

  /**
   * Set the subset of the replicas that are in sync
   *
   * @param isr  the subset of the replicas that are in sync
   * @return  current instance of the class to be fluent
   */
  public TopicPartitionInfo setIsr(List<Node> isr) {
    this.isr = isr;
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
  public TopicPartitionInfo setLeader(Node leader) {
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
  public TopicPartitionInfo setPartition(int partition) {
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
  public TopicPartitionInfo setReplicas(List<Node> replicas) {
    this.replicas = replicas;
    return this;
  }

  @Override
  public String toString() {

    return "PartitionInfo{" +
      "partition=" + this.partition +
      ", isr=" + this.isr +
      ", leader=" + this.leader +
      ", replicas=" + this.replicas +
      "}";
  }
}
