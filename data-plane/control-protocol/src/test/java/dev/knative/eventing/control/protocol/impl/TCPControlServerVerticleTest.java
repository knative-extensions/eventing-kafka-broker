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
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class TCPControlServerVerticleTest {

  private static final int port = 10001;

  private static final String INCOMING_EB_ADDRESS = "incoming.control.protocol";
  private static final String OUTGOING_EB_ADDRESS = "outgoing.control.protocol";

  @BeforeEach
  void setup(Vertx vertx) throws ExecutionException, InterruptedException {
    ControlMessageImplCodec.register(vertx.eventBus());
    vertx.deployVerticle(new TCPControlServerVerticle(port, INCOMING_EB_ADDRESS, OUTGOING_EB_ADDRESS))
      .toCompletionStage()
      .toCompletableFuture()
      .get();
  }

  @Test
  public void testReceiveAndAckWithEmpty(Vertx vertx) throws Exception {
    // Just echo with empty
    vertx.eventBus().consumer(INCOMING_EB_ADDRESS).handler(msg -> msg.reply(null));

    ControlMessage input = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();

    ControlMessage expected = new ControlMessageImpl.Builder()
      .setOpCode(ControlMessage.ACK_OP_CODE)
      .setUuid(input.uuid())
      .build();

    Socket socket = new Socket("localhost", port);
    List<ControlMessage> received = sendAndReceive(socket, input, expected.toBuffer().length());

    assertThat(received)
      .containsExactly(expected);
  }

  @Test
  public void testReceiveAndAckWithError(Vertx vertx) throws Exception {
    // Echo with error
    vertx.eventBus().consumer(INCOMING_EB_ADDRESS).handler(msg -> msg.reply("error"));

    ControlMessage input = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();

    ControlMessage expected = new ControlMessageImpl.Builder()
      .setOpCode(ControlMessage.ACK_OP_CODE)
      .setUuid(input.uuid())
      .setLengthAndPayload(Buffer.buffer("error"))
      .build();

    Socket socket = new Socket("localhost", port);
    List<ControlMessage> received = sendAndReceive(socket, input, expected.toBuffer().length());

    assertThat(received)
      .containsExactly(expected);
  }

  @Test
  public void testReceiveAndAckWithException(Vertx vertx) throws Exception {
    vertx.eventBus().registerDefaultCodec(IllegalStateException.class,
      new MessageCodec<IllegalStateException, IllegalStateException>() {
        @Override
        public void encodeToWire(Buffer buffer, IllegalStateException e) {
          throw new UnsupportedOperationException();
        }

        @Override
        public IllegalStateException decodeFromWire(int pos, Buffer buffer) {
          throw new UnsupportedOperationException();
        }

        @Override
        public IllegalStateException transform(IllegalStateException e) {
          return e;
        }

        @Override
        public String name() {
          return "blabla";
        }

        @Override
        public byte systemCodecID() {
          return -1;
        }
      });
    // Echo with exception
    vertx.eventBus().consumer(INCOMING_EB_ADDRESS).handler(msg -> msg.reply(new IllegalStateException("blabla")));

    ControlMessage input = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();

    ControlMessage expected = new ControlMessageImpl.Builder()
      .setOpCode(ControlMessage.ACK_OP_CODE)
      .setUuid(input.uuid())
      .setLengthAndPayload(Buffer.buffer(new IllegalStateException("blabla").getMessage()))
      .build();

    Socket socket = new Socket("localhost", port);
    List<ControlMessage> received = sendAndReceive(socket, input, expected.toBuffer().length());

    assertThat(received)
      .containsExactly(expected);
  }

  @Test
  public void testSendAndReceiveAck(Vertx vertx, VertxTestContext testContext) throws Exception {
    echoClient(null, vertx, testContext);

    ControlMessage input = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();

    ControlMessage actual = (ControlMessage) vertx.eventBus().request(OUTGOING_EB_ADDRESS, input)
      .toCompletionStage()
      .toCompletableFuture()
      .get()
      .body();

    assertThat(actual)
      .isEqualTo(
        new ControlMessageImpl.Builder()
          .setOpCode(ControlMessage.ACK_OP_CODE)
          .setUuid(input.uuid())
          .build()
      );

    testContext.completeNow();
  }

  @Test
  public void testSendAndReceiveAckWithError(Vertx vertx, VertxTestContext testContext) throws Exception {
    echoClient("error", vertx, testContext);

    ControlMessage input = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();

    ControlMessage actual = (ControlMessage) vertx.eventBus().request(OUTGOING_EB_ADDRESS, input)
      .toCompletionStage()
      .toCompletableFuture()
      .get()
      .body();

    assertThat(actual)
      .isEqualTo(
        new ControlMessageImpl.Builder()
          .setOpCode(ControlMessage.ACK_OP_CODE)
          .setUuid(input.uuid())
          .setLengthAndPayload(Buffer.buffer("error"))
          .build()
      );

    testContext.completeNow();
  }

  private List<ControlMessage> sendAndReceive(Socket socket, ControlMessage msg, int expectedToRead)
    throws IOException {
    List<ControlMessage> parsedMessages = new ArrayList<>();
    ControlMessageParser parser = new ControlMessageParser(parsedMessages::add);

    socket.getOutputStream().write(
      msg.toBuffer().getBytes()
    );

    parser.handle(
      Buffer.buffer(socket.getInputStream().readNBytes(expectedToRead))
    );

    return parsedMessages;
  }

  private void echoClient(String responsePayload, Vertx vertx, VertxTestContext testContext) {
    vertx.runOnContext(v ->
      vertx.createNetClient().connect(port, "localhost")
        .onFailure(testContext::failNow)
        .onSuccess(socket -> {
          // Handler when a complete message is received
          ControlMessageParser parser = new ControlMessageParser(receivedMsg -> {
            ControlMessageImpl.Builder builder = new ControlMessageImpl.Builder()
              .setOpCode(ControlMessage.ACK_OP_CODE)
              .setUuid(receivedMsg.uuid());
            if (responsePayload != null) {
              builder.setLengthAndPayload(Buffer.buffer(responsePayload));
            }

            Buffer buffer = builder.build().toBuffer();

            socket.write(buffer)
              .onFailure(testContext::failNow);
          });

          // Register the parser
          socket.handler(parser);
        })
    );
  }

}
