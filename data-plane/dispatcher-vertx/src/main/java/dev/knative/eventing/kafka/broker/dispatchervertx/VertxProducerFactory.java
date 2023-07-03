package dev.knative.eventing.kafka.broker.dispatchervertx;

import java.util.Map;

import dev.knative.eventing.kafka.broker.dispatcher.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.dispatcher.ReactiveProducerFactory;
import io.vertx.core.Vertx;

public class VertxProducerFactory<K, V> implements ReactiveProducerFactory<K, V>{

    @Override
    public ReactiveKafkaProducer<K, V> create(Vertx vertx, Map<String, Object> configs) {
        return new VertxKafkaProducer<>(vertx, configs);
    }
    
}
