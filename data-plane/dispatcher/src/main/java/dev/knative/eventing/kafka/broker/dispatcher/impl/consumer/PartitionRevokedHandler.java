package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import io.vertx.core.Future;
import io.vertx.kafka.client.common.TopicPartition;

import java.util.Set;

/**
 * {@link PartitionRevokedHandler} is the handler called when some partitions are revoked to a
 * {@link org.apache.kafka.clients.consumer.Consumer}
 */
public interface PartitionRevokedHandler {

  /**
   * @param partitions revoked partitions
   * @return a successful or a failed future
   */
  Future<Void> partitionRevoked(final Set<TopicPartition> partitions);
}
