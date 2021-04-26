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
import io.vertx.core.buffer.Buffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class ControlMessageParserTest {

  private static Stream<Arguments> twoMessagesWithSplitBuffers() {
    ControlMessage first = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();

    ControlMessage second = new ControlMessageImpl.Builder()
      .setOpCode((byte) 2)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("123abc"))
      .build();

    Buffer inputBuf = first.toBuffer();
    inputBuf.appendBuffer(second.toBuffer());

    return IntStream.range(1, inputBuf.length())
      .mapToObj(splitIndex -> Arguments.of(
        List.of(first, second),
        inputBuf.slice(0, splitIndex),
        inputBuf.slice(splitIndex, inputBuf.length()),
        splitIndex,
        inputBuf.length() - splitIndex
      ));
  }

  private static Stream<Arguments> threeMessagesWithSplitBuffers() {
    ControlMessage first = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();
    ControlMessage second = new ControlMessageImpl.Builder()
      .setOpCode((byte) 2)
      .setUuid(UUID.randomUUID())
      .build();
    ControlMessage third = new ControlMessageImpl.Builder()
      .setOpCode((byte) 3)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("123abc"))
      .build();

    Buffer inputBuf = first.toBuffer();
    inputBuf.appendBuffer(second.toBuffer());
    inputBuf.appendBuffer(third.toBuffer());

    return IntStream.range(1, inputBuf.length())
      .mapToObj(splitIndex -> Arguments.of(
        List.of(first, second, third),
        inputBuf.slice(0, splitIndex),
        inputBuf.slice(splitIndex, inputBuf.length()),
        splitIndex,
        inputBuf.length() - splitIndex
      ));
  }

  @Test
  public void testHandleCompleteMessage() throws Exception {
    List<ControlMessage> parsedMessages = new ArrayList<>();
    ControlMessageParser parser = new ControlMessageParser(parsedMessages::add);

    ControlMessage expected = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();

    parser.handle(expected.toBuffer());

    assertThat(parsedMessages)
      .containsExactly(expected);
  }

  @Test
  public void testHandleEmptyPayload() throws Exception {
    List<ControlMessage> parsedMessages = new ArrayList<>();
    ControlMessageParser parser = new ControlMessageParser(parsedMessages::add);

    ControlMessage expected = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .build();

    parser.handle(expected.toBuffer());

    assertThat(parsedMessages)
      .containsExactly(expected);
  }

  @Test
  public void testHandleMultipleMessages() throws Exception {
    List<ControlMessage> parsedMessages = new ArrayList<>();
    ControlMessageParser parser = new ControlMessageParser(parsedMessages::add);

    ControlMessage first = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("abc123"))
      .build();

    ControlMessage second = new ControlMessageImpl.Builder()
      .setOpCode((byte) 2)
      .setUuid(UUID.randomUUID())
      .setLengthAndPayload(Buffer.buffer("123abc"))
      .build();

    parser.handle(first.toBuffer());
    parser.handle(second.toBuffer());

    assertThat(parsedMessages)
      .containsExactly(first, second);
  }

  @Test
  public void testHandleMultipleMessagesWithoutPayload() throws Exception {
    List<ControlMessage> parsedMessages = new ArrayList<>();
    ControlMessageParser parser = new ControlMessageParser(parsedMessages::add);

    ControlMessage first = new ControlMessageImpl.Builder()
      .setOpCode((byte) 1)
      .setUuid(UUID.randomUUID())
      .build();

    ControlMessage second = new ControlMessageImpl.Builder()
      .setOpCode((byte) 2)
      .setUuid(UUID.randomUUID())
      .build();

    parser.handle(first.toBuffer());
    parser.handle(second.toBuffer());

    assertThat(parsedMessages)
      .containsExactly(first, second);
  }

  @ParameterizedTest(name = "with first length {3} and second length {4}")
  @MethodSource("twoMessagesWithSplitBuffers")
  public void testTwoMessagesWithSplitBuffers(List<ControlMessage> expected, Buffer firstBuffer, Buffer secondBuffer,
                                              int firstBufferLength, int secondBufferLength) {
    List<ControlMessage> parsedMessages = new ArrayList<>();
    ControlMessageParser parser = new ControlMessageParser(parsedMessages::add);

    parser.handle(firstBuffer);
    parser.handle(secondBuffer);

    assertThat(parsedMessages)
      .isEqualTo(expected);
  }

  @ParameterizedTest(name = "with first length {3} and second length {4}")
  @MethodSource("threeMessagesWithSplitBuffers")
  public void testThreeMessagesWithSplitBuffers(List<ControlMessage> expected, Buffer firstBuffer, Buffer secondBuffer,
                                                int firstBufferLength, int secondBufferLength) {
    List<ControlMessage> parsedMessages = new ArrayList<>();
    ControlMessageParser parser = new ControlMessageParser(parsedMessages::add);

    parser.handle(firstBuffer);
    parser.handle(secondBuffer);

    assertThat(parsedMessages)
      .isEqualTo(expected);
  }

}
