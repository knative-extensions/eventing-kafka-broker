package dev.knative.eventing.kafka.broker.dispatcher;

import java.util.Map;

import io.vertx.core.Vertx;

/**
 * A factory interface for creating reactive Kafka consumers.
 *
 * @param <K> The type of the Kafka message key.
 * @param <V> The type of the Kafka message value.
 */
public interface ReactiveConsumerFactory<K, V> {

    /**
     * Creates a new reactive Kafka consumer using the provided Vertx instance and configuration.
     *
     * @param vertx   The Vertx instance to be used by the vertx consumer only.
     * @param configs The configuration options for the consumer.
     * @return The created reactive Kafka consumer.
     */
    ReactiveKafkaConsumer<K, V> create(Vertx vertx, Map<String, Object> configs);
}

