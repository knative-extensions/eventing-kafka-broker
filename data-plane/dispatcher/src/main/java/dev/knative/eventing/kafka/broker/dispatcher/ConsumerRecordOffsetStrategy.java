package dev.knative.eventing.kafka.broker.dispatcher;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public interface ConsumerRecordOffsetStrategy<K, V> {

  /**
   * The given record has been received.
   *
   * @param record record received.
   */
  void recordReceived(KafkaConsumerRecord<K, V> record);

  /**
   * The given record has been successfully sent to subscriber.
   *
   * @param record record sent to subscriber.
   */
  void successfullySentToSubscriber(KafkaConsumerRecord<K, V> record);

  /**
   * The given record has been successfully sent to dead letter queue.
   *
   * @param record record sent to dead letter queue.
   */
  void successfullySentToDLQ(KafkaConsumerRecord<K, V> record);

  /**
   * The given record cannot be delivered to dead letter queue.
   *
   * @param record record undeliverable to dead letter queue.
   * @param ex     exception occurred.
   */
  void failedToSendToDLQ(KafkaConsumerRecord<K, V> record, Throwable ex);

  /**
   * The given event doesn't pass the filter.
   *
   * @param record record discarded.
   */
  void recordDiscarded(KafkaConsumerRecord<K, V> record);
}
