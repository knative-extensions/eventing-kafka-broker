package dev.knative.eventing.kafka.broker.core.cloudevents;

import io.cloudevents.CloudEvent;

public final class PartitionKey {

  public static final String PARTITION_KEY_KEY = "partitionkey";

  public static final String PARTITION_KEY_DEFAULT = "";

  /**
   * Extract the partitionkey extension from the given event.
   *
   * @param event event from which extracting partition key.
   * @return partitionkey extension value or PARTITION_KEY_DEFAULT if not set
   * @link https://github.com/cloudevents/spec/blob/master/extensions/partitioning.md
   */
  public static String extract(final CloudEvent event) {

    final var partitionKey = event.getExtension(PARTITION_KEY_KEY);
    if (partitionKey == null) {
      return PARTITION_KEY_DEFAULT;
    }

    return partitionKey.toString();
  }
}
