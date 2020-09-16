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

import com.google.protobuf.Empty;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.cloudevents.CloudEvent;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Execution(value = ExecutionMode.CONCURRENT)
public class EgressWrapperTest {

  @ParameterizedTest
  @MethodSource("equalEgressProvider")
  public void testEgressEquality(final EgressWrapper t1, final EgressWrapper t2) {
    assertThat(t1).isEqualTo(t2);
    assertThat(t1.hashCode()).isEqualTo(t2.hashCode());
  }

  @ParameterizedTest
  @MethodSource("differentEgressProvider")
  public void testEgressDifference(final EgressWrapper t1, final EgressWrapper t2) {
    assertThat(t1).isNotEqualTo(t2);
    assertThat(t1.hashCode()).isNotEqualTo(t2.hashCode());
  }

  @Test
  public void consumerGroupCallShouldBeDelegatedToWrappedEgress() {
    final var consumerGroup = "123-42";
    final var egressWrapper = new EgressWrapper(
      DataPlaneContract.Egress.newBuilder().setConsumerGroup(consumerGroup).build()
    );

    assertThat(egressWrapper.consumerGroup()).isEqualTo(consumerGroup);
  }

  @Test
  public void destinationCallShouldBeDelegatedToWrappedEgress() {
    final var destination = "destination-42";
    final var egressWrapper = new EgressWrapper(
      DataPlaneContract.Egress.newBuilder().setDestination(destination).build()
    );

    assertThat(egressWrapper.destination()).isEqualTo(destination);
  }

  @Test
  public void replyToUrlCallShouldBeDelegatedToWrappedEgress() {
    final var replyToUrl = "123-42";
    final var egressWrapper = new EgressWrapper(
      DataPlaneContract.Egress.newBuilder().setReplyUrl(replyToUrl).build()
    );

    assertThat(egressWrapper.isReplyToUrl())
      .isTrue();
    assertThat(egressWrapper.isReplyToOriginalTopic())
      .isFalse();
    assertThat(egressWrapper.replyUrl())
      .isEqualTo(replyToUrl);
  }

  @Test
  public void replyToOriginalTopicCallShouldBeDelegatedToWrappedEgress() {
    final var egressWrapper = new EgressWrapper(
      DataPlaneContract.Egress.newBuilder().setReplyToOriginalTopic(Empty.newBuilder()).build()
    );

    assertThat(egressWrapper.isReplyToUrl())
      .isFalse();
    assertThat(egressWrapper.isReplyToOriginalTopic())
      .isTrue();
  }

  // test if filter returned by filter() agrees with EventMatcher
  @ParameterizedTest
  @MethodSource(value = "dev.knative.eventing.kafka.broker.core.EventMatcherTest#testCases")
  public void testFilter(
    final Map<String, String> attributes,
    final CloudEvent event,
    final boolean shouldMatch) {
    final var egressWrapper = new EgressWrapper(
      DataPlaneContract.Egress.newBuilder()
        .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(attributes))
        .build()
    );

    final var filter = egressWrapper.filter();

    final var match = filter.match(event);

    assertThat(match).isEqualTo(shouldMatch);
  }

  public static Stream<Arguments> differentEgressProvider() {
    return Stream.of(
      // egress' destination is different
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes((Collections.emptyMap())))
          .setDestination("this-is-my-destination1")
          .setConsumerGroup("1234-hello")
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes((Collections.emptyMap())))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello")
          .build()
        )
      ),
      // egress' attributes are different
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion1",
            "1.0",
            "type",
            "type_value"
          )))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello")
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type_value"
          )))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello")
          .build()
        )
      ),
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type_value1"
          )))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello")
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type_value"
          )))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello")
          .build()
        )
      ),
      // egress' id is different
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type_value"
          )))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello1")
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type_value"
          )))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello")
          .build()
        )
      )
    );
  }

  public static Stream<Arguments> equalEgressProvider() {
    return Stream.of(
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type_value"
          )))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello")
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type_value"
          )))
          .setDestination("this-is-my-destination")
          .setConsumerGroup("1234-hello")
          .build()
        )
      ),
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .build()
        )
      ),
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setConsumerGroup("1234")
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setConsumerGroup("1234")
          .build()
        )
      ),
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .build()
        )
      ),
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setDestination("dest")
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setDestination("dest")
          .build()
        )
      ),
      Arguments.of(
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type1"
          )))
          .build()
        ),
        new EgressWrapper(DataPlaneContract.Egress
          .newBuilder()
          .setFilter(DataPlaneContract.Filter.newBuilder().putAllAttributes(Map.of(
            "specversion",
            "1.0",
            "type",
            "type1"
          )))
          .build()
        )
      )
    );
  }
}
