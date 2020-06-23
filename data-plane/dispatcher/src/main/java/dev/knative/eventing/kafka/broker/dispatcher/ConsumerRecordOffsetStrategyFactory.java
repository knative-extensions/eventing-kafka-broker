package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.Trigger;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public interface ConsumerRecordOffsetStrategyFactory<K, V> {

  static <K, V> ConsumerRecordOffsetStrategyFactory<K, V> create() {
    return new ConsumerRecordOffsetStrategyFactory<>() {
    };
  }

  default ConsumerRecordOffsetStrategy<K, V> get(
      final KafkaConsumer<K, V> consumer,
      final Broker broker,
      final Trigger<V> trigger) {

    return new UnorderedConsumerRecordOffsetStrategy<>(consumer);
  }
}
