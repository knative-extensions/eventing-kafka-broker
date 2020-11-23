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
package dev.knative.eventing.kafka.broker.core.cloudevents;

import static org.assertj.core.api.Assertions.assertThat;

import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class PartitionKeyTest {

  @Test
  public void returnDefaultPartitionKey() {

    final var event = CloudEventBuilder.v1()
      .withId(UUID.randomUUID().toString())
      .withType("Type1")
      .withSource(URI.create("source"))
      .build();

    final var partitionKey = PartitionKey.extract(event);

    assertThat(partitionKey).isNull();
  }

  @Test
  public void returnPartitionKeyExtension() {

    final var expectedPartitionKeyValue = "Rossi";

    final var event = CloudEventBuilder.v1()
      .withId(UUID.randomUUID().toString())
      .withType("Type1")
      .withSource(URI.create("source"))
      .withExtension(PartitionKey.PARTITION_KEY_KEY, expectedPartitionKeyValue)
      .build();

    final var partitionKey = PartitionKey.extract(event);

    assertThat(partitionKey).isEqualTo(expectedPartitionKeyValue);
  }
}
