package dev.knative.eventing.kafka.broker.dispatcher;

import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

@FunctionalInterface
public interface ConsumerRecordSender<K, V, R> {

  /**
   * Send the given record. (the record passed the filter)
   *
   * @param record record to send
   * @return a successful future or a failed future.
   */
  Future<R> send(KafkaConsumerRecord<K, V> record);
}
