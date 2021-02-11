package dev.knative.eventing.kafka.broker.dispatcher.consumer;

public enum ConsumerType {
  /**
   * Ordered consumer is a per-partition blocking consumer that deliver messages in order.
   */
  ORDERED,
  /**
   * Unordered consumer is a non-blocking consumer that potentially deliver messages unordered, while preserving proper offset management.
   */
  UNORDERED
}
