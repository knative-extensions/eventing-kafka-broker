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
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPControlServerVerticle extends AbstractVerticle {

  private final static DeliveryOptions DELIVERY_OPTIONS = new DeliveryOptions().setLocalOnly(true);
  private static final Logger logger = LoggerFactory.getLogger(TCPControlServerVerticle.class);

  private final int port;
  private final String incomingMessageAddress;
  private final String outgoingMessageAddress;
  private final Map<UUID, Message<ControlMessage>> toAck;
  private final Queue<ControlMessage> enqueuedWaitingForConnection;

  private NetServer server;
  private NetSocket actualConnection;

  public TCPControlServerVerticle(int port, String incomingMessageAddress, String outgoingMessageAddress) {
    this.port = port;
    this.incomingMessageAddress = incomingMessageAddress;
    this.outgoingMessageAddress = outgoingMessageAddress;
    this.toAck = new HashMap<>();
    this.enqueuedWaitingForConnection = new ArrayDeque<>();
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    // Start the event bus message listener
    vertx.eventBus().<ControlMessage>localConsumer(this.outgoingMessageAddress)
      .handler(this::handleOutboundMessage);

    // Start the tcp server
    this.server = vertx.createNetServer(createNetServerOptions());
    server.connectHandler(this::newConnectionHandler);
    server.exceptionHandler(t -> logger.error("Error in tcp server", t));
    server
      .listen()
      .<Void>mapEmpty()
      .onComplete(startPromise);
  }

  private NetServerOptions createNetServerOptions() {
    return new NetServerOptions()
      .setPort(port)
      .setTcpKeepAlive(true);
  }

  private void newConnectionHandler(NetSocket netSocket) {
    if (actualConnection != null) {
      // Close the previous connection,
      // the control protocol assumes only one connection is open at any given time
      actualConnection.close();
    }
    actualConnection = netSocket;
    actualConnection.handler(new ControlMessageParser(this::handleInboundMessage));
    actualConnection.exceptionHandler(t -> logger.error("Error in connection", t));

    // Dequeue the elements waiting for a connection
    while (!enqueuedWaitingForConnection.isEmpty()) {
      writeOnConnection(enqueuedWaitingForConnection.poll());
    }
  }

  private void handleInboundMessage(ControlMessage message) {
    if (message.opCode() == MessageConstants.ACK_OP_CODE) {
      handleInboundAck(message.uuid());
    } else {
      emitMessageOnEventBus(message);
    }
  }

  private void handleInboundAck(UUID uuid) {
    Message<ControlMessage> message = this.toAck.get(uuid);
    if (message == null) {
      logger.warn("Received an ack for an unknown uuid {}", uuid);
    } else {
      message.reply(message.body());
    }
  }

  private void emitMessageOnEventBus(ControlMessage message) {
    // TODO should the message delivery be serialized using AsyncOrderedExecutor?
    //  Or should "downstream" take care of that?
    //  Or should we provide an OrderedMessageHandler for the event bus or similar?
    //  Note: that's the behaviour of the Golang client
    vertx.eventBus().request(this.incomingMessageAddress, message, DELIVERY_OPTIONS)
      .onFailure(t -> logger
        .error("Cannot route the incoming control message to {}: {}", this.incomingMessageAddress, message, t))
      .onSuccess(m -> sendAckBack(message.uuid()));
  }

  private void sendAckBack(UUID uuid) {
    ControlMessage message = new ControlMessageImpl.Builder()
      .setOpCode(MessageConstants.ACK_OP_CODE)
      .setUuid(uuid)
      .build();
    writeOnConnection(message);
  }

  private void handleOutboundMessage(Message<ControlMessage> message) {
    ControlMessage controlMessage = message.body();
    writeOnConnection(controlMessage);
    this.toAck.put(controlMessage.uuid(), message);
  }

  private void writeOnConnection(ControlMessage message) {
    if (actualConnection == null) {
      // Enqueue the message
      this.enqueuedWaitingForConnection.offer(message);
    }
    this.internalWriteOnConnection(message, message.toBuffer());
  }

  private void internalWriteOnConnection(ControlMessage message, Buffer buffer) {
    actualConnection.write(buffer)
      .onSuccess(v -> logger.debug("Successfully sent {}", message))
      .onFailure(t -> {
        logger.error("Cannot dispatch message {}", message, t);
        // TODO should we retry here?
      });
  }

}
