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
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class CloudEventOverridesMutatorTest {

  @Test
  public void shouldAddExtensions() {
    final var extensions = Map.of(
      "a", "foo",
      "b", "bar"
    );
    final var ceOverrides = DataPlaneContract.CloudEventOverrides
      .newBuilder()
      .putAllExtensions(extensions)
      .build();

    final var mutator = new CloudEventOverridesMutator(ceOverrides);

    final var given = CloudEventBuilder.v1()
      .withId(UUID.randomUUID().toString())
      .withSource(URI.create("/v1/api"))
      .withTime(OffsetDateTime.MIN)
      .withType("foo")
      .build();

    final var expected = CloudEventBuilder.from(given);
    extensions.forEach(expected::withExtension);

    final var got = mutator.apply(given);

    assertThat(got).isEqualTo(expected.build());
  }

  @Test
  public void shouldNotMutateRecordWhenNoOverrides() {
    final var ceOverrides = DataPlaneContract.CloudEventOverrides
      .newBuilder()
      .putAllExtensions(Map.of())
      .build();

    final var mutator = new CloudEventOverridesMutator(ceOverrides);

    final var given = CloudEventBuilder.v1()
      .withId(UUID.randomUUID().toString())
      .withSource(URI.create("/v1/api"))
      .withTime(OffsetDateTime.MIN)
      .withType("foo")
      .build();

    final var got = mutator.apply(given);

    assertThat(got).isSameAs(given);
  }
}
