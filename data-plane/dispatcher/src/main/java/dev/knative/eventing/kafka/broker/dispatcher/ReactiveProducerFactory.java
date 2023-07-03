package dev.knative.eventing.kafka.broker.dispatcher;

import java.util.Map;
import io.vertx.core.Vertx;


/**
 * Factory for creating ReactiveKafkaProducer
 * 
 * @param <K> the key type
 * @param <V> the value type
 */
public interface ReactiveProducerFactory<K, V> {

    /**
     * Create a new ReactiveKafkaProducer
     *
     * @param v        the Vertx instance used when creating the vertx KafkaProducer
     * @param producer the Kafka producer
     * @return a new ReactiveKafkaProducer
     */
    ReactiveKafkaProducer<K, V> create(final Vertx vertx, final Map<String, Object> configs);

}
