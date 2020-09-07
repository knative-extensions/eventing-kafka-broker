/*
 * Copyright 2020 The Knative Authors
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

package dev.knative.eventing.kafka.broker.core;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.config.BrokersConfig;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.ContentMode;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BrokerWrapperTest {

  @Test
  public void idCallShouldBeDelegatedToWrappedBroker() {
    final var id = "123-42";
    final var broker = new BrokerWrapper(
      Broker.newBuilder().setId(id).build()
    );

    assertThat(broker.id()).isEqualTo(id);
  }

  @Test
  public void deadLetterSinkCallShouldBeDelegatedToWrappedBroker() {
    final var deadLetterSink = "http://localhost:9090/api";
    final var broker = new BrokerWrapper(
      Broker.newBuilder().setDeadLetterSink(deadLetterSink).build()
    );

    assertThat(broker.deadLetterSink()).isEqualTo(deadLetterSink);
  }

  @Test
  public void topicCallShouldBeDelegatedToWrappedBroker() {
    final var topic = "knative-topic";
    final var broker = new BrokerWrapper(
      Broker.newBuilder().setTopic(topic).build()
    );

    assertThat(broker.topic()).isEqualTo(topic);
  }

  @ParameterizedTest
  @MethodSource(value = {"equalTriggersProvider"})
  public void testTriggerEquality(
    final dev.knative.eventing.kafka.broker.core.Broker b1,
    final dev.knative.eventing.kafka.broker.core.Broker b2) {

    assertThat(b1).isEqualTo(b2);
    assertThat(b1.hashCode()).isEqualTo(b2.hashCode());
  }

  @ParameterizedTest
  @MethodSource(value = {"differentTriggersProvider"})
  public void testTriggerDifference(
    final dev.knative.eventing.kafka.broker.core.Broker b1,
    final dev.knative.eventing.kafka.broker.core.Broker b2) {

    assertThat(b1).isNotEqualTo(b2);
    assertThat(b1.hashCode()).isNotEqualTo(b2.hashCode());
  }

  public static Stream<Arguments> differentTriggersProvider() {
    return Stream.of(
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setId("1234-id")
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder().build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setTopic("kantive-topic")
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder().build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setDeadLetterSink("http:/localhost:9090")
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder().build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setBootstrapServers("kafka-1:9092,kafka-2:9093")
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder()
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setContentMode(ContentMode.BINARY)
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder()
            .setContentMode(ContentMode.STRUCTURED)
            .build()
        )
      )
    );
  }

  public static Stream<Arguments> equalTriggersProvider() {
    return Stream.of(
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder().build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setPath("/broker/test-event-transformation-for-trigger-v1-broker-v1-6wlx9")
            .setId("93ab71bd-9e3c-42ee-a0a2-aeec6dca48c9")
            .setTopic(
              "knative-broker-test-event-transformation-for-trigger-v1-broker-v1-6wlx9-broker")
            .addTriggers(BrokersConfig.Trigger.newBuilder()
              .setDestination(
                "http://trans-pod.test-event-transformation-for-trigger-v1-broker-v1-6wlx9.svc.cluster.local/")
              .setId("9bfd1eb6-5f09-49cf-b3fa-b70377aff64f")
              .putAttributes("source", "source1")
              .putAttributes("type", "type1")
              .build())
            .addTriggers(BrokersConfig.Trigger.newBuilder()
              .setDestination(
                "http://recordevents-pod.test-event-transformation-for-trigger-v1-broker-v1-6wlx9.svc.cluster.local/")
              .setId("2417e90f-fc9a-430b-a9f7-68ee3b5929cd")
              .putAttributes("source", "source2")
              .putAttributes("type", "type2")
              .build())
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder()
            .setPath("/broker/test-event-transformation-for-trigger-v1-broker-v1-6wlx9")
            .setId("93ab71bd-9e3c-42ee-a0a2-aeec6dca48c9")
            .setTopic(
              "knative-broker-test-event-transformation-for-trigger-v1-broker-v1-6wlx9-broker")
            .addTriggers(BrokersConfig.Trigger.newBuilder()
              .setDestination(
                "http://trans-pod.test-event-transformation-for-trigger-v1-broker-v1-6wlx9.svc.cluster.local/")
              .setId("9bfd1eb6-5f09-49cf-b3fa-b70377aff64f")
              .putAttributes("source", "source1")
              .putAttributes("type", "type1")
              .build())
            .addTriggers(BrokersConfig.Trigger.newBuilder()
              .setDestination(
                "http://recordevents-pod.test-event-transformation-for-trigger-v1-broker-v1-6wlx9.svc.cluster.local/")
              .setId("2417e90f-fc9a-430b-a9f7-68ee3b5929cd")
              .putAttributes("source", "source2")
              .putAttributes("type", "type2")
              .build())
            .build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setTopic("knative-topic")
            .setId("1234-42")
            .setDeadLetterSink("http://localhost:9090")
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder()
            .setId("1234-42")
            .setTopic("knative-topic")
            .setDeadLetterSink("http://localhost:9090")
            .build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder()
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setContentMode(ContentMode.BINARY)
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder()
            .setContentMode(ContentMode.BINARY)
            .build()
        )
      ),
      Arguments.of(
        new BrokerWrapper(
          Broker.newBuilder()
            .setContentMode(ContentMode.STRUCTURED)
            .build()
        ),
        new BrokerWrapper(
          Broker.newBuilder()
            .setContentMode(ContentMode.STRUCTURED)
            .build()
        )
      )
    );
  }
}
