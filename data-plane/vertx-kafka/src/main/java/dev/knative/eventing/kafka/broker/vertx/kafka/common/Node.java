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
 * Information about a Kafka cluster node
 */
@DataObject
public class Node {

  private boolean hasRack;
  private String host;
  private int id;
  private String idString;
  private boolean isEmpty;
  private int port;
  private String rack;

  /**
   * Constructor
   */
  public Node() {

  }

  /**
   * Constructor
   *
   * @param hasRack true if this node has a defined rack
   * @param host  the host name for this node
   * @param id  the node id of this node
   * @param idString  String representation of the node id
   * @param isEmpty if this node is empty, which may be the case if noNode() is used as a placeholder in a response payload with an error
   * @param port  the port for this node
   * @param rack  the rack for this node
   */
  public Node(boolean hasRack, String host, int id, String idString, boolean isEmpty, int port, String rack) {
    this.hasRack = hasRack;
    this.host = host;
    this.id = id;
    this.idString = idString;
    this.isEmpty = isEmpty;
    this.port = port;
    this.rack = rack;
  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public Node(JsonObject json) {
    this.hasRack = json.getBoolean("hasRack");
    this.host = json.getString("host");
    this.id = json.getInteger("id");
    this.idString = json.getString("idString");
    this.isEmpty = json.getBoolean("isEmpty");
    this.port = json.getInteger("port");
    this.rack = json.getString("rack");
  }

  /**
   * @return  true if this node has a defined rack
   */
  public boolean hasRack() {
    return this.hasRack;
  }

  /**
   * Set if this node has a defined rack
   *
   * @param hasRack if this node has a defined rack
   * @return  current instance of the class to be fluent
   */
  public Node setHasRack(boolean hasRack) {
    this.hasRack = hasRack;
    return this;
  }

  /**
   * @return  the host name for this node
   */
  public String getHost() {
    return this.host;
  }

  /**
   * Set the host name for this node
   *
   * @param host  the host name for this node
   * @return  current instance of the class to be fluent
   */
  public Node setHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * @return  the node id of this node
   */
  public int getId() {
    return this.id;
  }

  /**
   * Set the node id of this node
   *
   * @param id  the node id of this node
   * @return  current instance of the class to be fluent
   */
  public Node setId(int id) {
    this.id = id;
    return this;
  }

  /**
   * @return  String representation of the node id
   */
  public String getIdString() {
    return this.idString;
  }

  /**
   * Set the string representation of the node id
   *
   * @param idString  String representation of the node id
   * @return  current instance of the class to be fluent
   */
  public Node setIdString(String idString) {
    this.idString = idString;
    return this;
  }

  /**
   * @return  if this node is empty, which may be the case if noNode() is used as a placeholder in a response payload with an error
   */
  public boolean isEmpty() {
    return this.isEmpty;
  }

  /**
   * Set if this node is empty
   *
   * @param isEmpty if this node is empty
   * @return  current instance of the class to be fluent
   */
  public Node setIsEmpty(boolean isEmpty) {
    this.isEmpty = isEmpty;
    return this;
  }

  /**
   * @return  the port for this node
   */
  public int getPort() {
    return this.port;
  }

  /**
   * Set the port for this node
   *
   * @param port  the port for this node
   * @return  current instance of the class to be fluent
   */
  public Node setPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * @return  the rack for this node
   */
  public String rack() {
    return this.rack;
  }

  /**
   * Set the rack for this node
   *
   * @param rack  the rack for this node
   * @return  current instance of the class to be fluent
   */
  public Node setRack(String rack) {
    this.rack = rack;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    JsonObject jsonObject = new JsonObject();

    jsonObject
      .put("hasRack", this.hasRack)
      .put("host", this.host)
      .put("id", this.id)
      .put("idString", this.idString)
      .put("isEmpty", this.isEmpty)
      .put("port", this.port)
      .put("rack", this.rack);

    return jsonObject;
  }

  @Override
  public String toString() {

    return "Node{" +
            "id=" + this.id +
            ", host=" + this.host +
            ", hasRack=" + this.hasRack +
            ", idString=" + this.idString +
            ", isEmpty=" + this.isEmpty +
            ", port=" + this.port +
            ", rack=" + this.rack +
            "}";
  }
}
