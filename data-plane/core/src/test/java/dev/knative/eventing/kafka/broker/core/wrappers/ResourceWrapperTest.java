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

package dev.knative.eventing.kafka.broker.core.wrappers;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ResourceWrapperTest {

  @Test
  public void idCallShouldBeDelegatedToWrappedResource() {
    final var id = "123-42";
    final var resource = new ResourceWrapper(
      DataPlaneContract.Resource.newBuilder().setUid(id).build()
    );

    assertThat(resource.id()).isEqualTo(id);
  }

  @Test
  public void topicsCallShouldBeDelegatedToWrappedResource() {
    final var topics = Set.of("knative-topic");
    final var resource = new ResourceWrapper(
      DataPlaneContract.Resource.newBuilder().addTopics(topics.iterator().next()).build()
    );

    assertThat(resource.topics()).isEqualTo(topics);
  }

  @Test
  public void bootstrapServersCallShouldBeDelegatedToWrappedResource() {
    final var bootstrapServers = "http://example.com";
    final var resource = new ResourceWrapper(
      DataPlaneContract.Resource.newBuilder().setBootstrapServers(bootstrapServers).build()
    );

    assertThat(resource.bootstrapServers()).isEqualTo(bootstrapServers);
  }

  @Test
  public void ingressCallShouldBeDelegatedToWrappedResource() {
    final var ingress = DataPlaneContract.Ingress.newBuilder().setPath("http://example.com").build();
    final var resource = new ResourceWrapper(
      DataPlaneContract.Resource.newBuilder().setIngress(ingress).build()
    );

    assertThat(resource.ingress()).isEqualTo(ingress);
  }

  @Test
  public void egressConfigCallShouldBeDelegatedToWrappedResource() {
    final var egressConfig = DataPlaneContract.EgressConfig.newBuilder().setDeadLetter("http://example.com").build();
    final var resource = new ResourceWrapper(
      DataPlaneContract.Resource.newBuilder().setEgressConfig(egressConfig).build()
    );

    assertThat(resource.egressConfig()).isEqualTo(egressConfig);
  }

  @ParameterizedTest
  @MethodSource("equalEgressProvider")
  public void testEgressEquality(
    final Resource b1,
    final Resource b2) {

    assertThat(b1).isEqualTo(b2);
    assertThat(b1.hashCode()).isEqualTo(b2.hashCode());
  }

  @ParameterizedTest
  @MethodSource("differentEgressProvider")
  public void testEgressDifference(
    final Resource b1,
    final Resource b2) {

    assertThat(b1).isNotEqualTo(b2);
    assertThat(b1.hashCode()).isNotEqualTo(b2.hashCode());
  }

  public static Stream<Arguments> differentEgressProvider() {
    return Stream.of(
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setUid("1234-id")
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder().build()
        )
      ),
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .addTopics("knative-topic")
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder().build()
        )
      ),
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setBootstrapServers("kafka-1:9092,kafka-2:9093")
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .build()
        )
      ),
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setIngress(
              DataPlaneContract.Ingress.newBuilder().setContentMode(DataPlaneContract.ContentMode.BINARY)
            )
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setIngress(
              DataPlaneContract.Ingress.newBuilder().setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
            )
            .build()
        )
      )
    );
  }

  public static Stream<Arguments> equalEgressProvider() {
    return Stream.of(
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder().build()
        )
      ),
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setIngress(DataPlaneContract.Ingress.newBuilder()
              .setPath("/broker/test-event-transformation-for-trigger-v1-broker-v1-6wlx9"))
            .setUid("93ab71bd-9e3c-42ee-a0a2-aeec6dca48c9")
            .addTopics(
              "knative-broker-test-event-transformation-for-trigger-v1-broker-v1-6wlx9-broker")
            .addEgresses(DataPlaneContract.Egress.newBuilder()
              .setDestination(
                "http://trans-pod.test-event-transformation-for-trigger-v1-broker-v1-6wlx9.svc.cluster.local/")
              .setConsumerGroup("9bfd1eb6-5f09-49cf-b3fa-b70377aff64f")
              .setFilter(DataPlaneContract.Filter.newBuilder()
                .putAttributes("source", "source1")
                .putAttributes("type", "type1")
              )
            )
            .addEgresses(DataPlaneContract.Egress.newBuilder()
              .setDestination(
                "http://recordevents-pod.test-event-transformation-for-trigger-v1-broker-v1-6wlx9.svc.cluster.local/")
              .setConsumerGroup("2417e90f-fc9a-430b-a9f7-68ee3b5929cd")
              .setFilter(DataPlaneContract.Filter.newBuilder()
                .putAttributes("source", "source2")
                .putAttributes("type", "type2")
              )
            )
            .setEgressConfig(DataPlaneContract.EgressConfig.newBuilder().setDeadLetter("http://localhost:9090"))
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setIngress(DataPlaneContract.Ingress.newBuilder()
              .setPath("/broker/test-event-transformation-for-trigger-v1-broker-v1-6wlx9"))
            .setUid("93ab71bd-9e3c-42ee-a0a2-aeec6dca48c9")
            .addTopics(
              "knative-broker-test-event-transformation-for-trigger-v1-broker-v1-6wlx9-broker")
            .addEgresses(DataPlaneContract.Egress.newBuilder()
              .setDestination(
                "http://trans-pod.test-event-transformation-for-trigger-v1-broker-v1-6wlx9.svc.cluster.local/")
              .setConsumerGroup("9bfd1eb6-5f09-49cf-b3fa-b70377aff64f")
              .setFilter(DataPlaneContract.Filter.newBuilder()
                .putAttributes("source", "source1")
                .putAttributes("type", "type1")
              )
            )
            .addEgresses(DataPlaneContract.Egress.newBuilder()
              .setDestination(
                "http://recordevents-pod.test-event-transformation-for-trigger-v1-broker-v1-6wlx9.svc.cluster.local/")
              .setConsumerGroup("2417e90f-fc9a-430b-a9f7-68ee3b5929cd")
              .setFilter(DataPlaneContract.Filter.newBuilder()
                .putAttributes("source", "source2")
                .putAttributes("type", "type2")
              )
            )
            .setEgressConfig(DataPlaneContract.EgressConfig.newBuilder().setDeadLetter("http://localhost:9090"))
            .build()
        )
      ),
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .build()
        )
      ),
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setIngress(DataPlaneContract.Ingress.newBuilder().setContentMode(DataPlaneContract.ContentMode.BINARY))
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setIngress(DataPlaneContract.Ingress.newBuilder().setContentMode(DataPlaneContract.ContentMode.BINARY))
            .build()
        )
      ),
      Arguments.of(
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setIngress(DataPlaneContract.Ingress.newBuilder().setContentMode(DataPlaneContract.ContentMode.STRUCTURED))
            .build()
        ),
        new ResourceWrapper(
          DataPlaneContract.Resource.newBuilder()
            .setIngress(DataPlaneContract.Ingress.newBuilder().setContentMode(DataPlaneContract.ContentMode.STRUCTURED))
            .build()
        )
      )
    );
  }
}
