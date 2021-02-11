package dev.knative.eventing.kafka.broker.dispatcher.consumer;

import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.OrderedOffsetManager;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.UnorderedConsumerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.impl.UnorderedOffsetManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.function.Consumer;

public enum ConsumerType {
  /**
   * Ordered consumer is a per-partition blocking consumer that deliver messages in order.
   */
  ORDERED,
  /**
   * Unordered consumer is a non-blocking consumer that potentially deliver messages unordered, while preserving proper offset management.
   */
  UNORDERED;

  public OffsetManager getOffsetManager(final KafkaConsumer<?, ?> consumer, Consumer<Integer> commitHandler) {
    return switch (this) {
      case ORDERED -> new OrderedOffsetManager(consumer, commitHandler);
      case UNORDERED -> new UnorderedOffsetManager(consumer, commitHandler);
    };
  }

  public AbstractVerticle getConsumerVerticle(final KafkaConsumer<?, ?> consumer) {
    return switch (this) {
      case ORDERED -> new UnorderedConsumerVerticle(consumer, commitHandler); //TODO
      case UNORDERED -> new UnorderedConsumerVerticle(consumer, commitHandler);
    };
  }

}
