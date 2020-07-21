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

    assertThat(partitionKey).isEqualTo(PartitionKey.PARTITION_KEY_DEFAULT);
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