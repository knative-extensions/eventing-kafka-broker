package dev.knative.eventing.kafka.broker.receiververtx;

import java.util.Properties;

import dev.knative.eventing.kafka.broker.receiver.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.receiver.ReactiveProducerFactory;
import io.vertx.core.Vertx;

public class VertxProducerFactory<K,V> implements ReactiveProducerFactory<K,V> {

    @Override
    public ReactiveKafkaProducer<K, V> create(Vertx v, Properties config) {
        return new VertxKafkaProducer<K,V>(v,config);
    }
    
}
