package dev.knative.eventing.kafka.broker.dispatcher;

import java.util.Map;

import org.apache.kafka.clients.producer.Producer;

import io.vertx.core.Vertx;

public class MockReactiveProducerFactory<K, V> implements ReactiveProducerFactory<K, V> {

    @Override
    public ReactiveKafkaProducer<K, V> create(Vertx vertx, Map<String, Object> configs) {
        return new MockReactiveKafkaProducer<>();
    }

    public ReactiveKafkaProducer<K, V> create(Vertx vertx, Producer<K, V> producer) {
        return new MockReactiveKafkaProducer<>(vertx, producer);
    }
    
}
