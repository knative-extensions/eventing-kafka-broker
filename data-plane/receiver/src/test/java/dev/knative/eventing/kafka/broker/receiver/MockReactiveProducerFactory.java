package dev.knative.eventing.kafka.broker.receiver;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;

import io.cloudevents.CloudEvent;
import io.vertx.core.Vertx;

public class MockReactiveProducerFactory implements ReactiveProducerFactory<String, CloudEvent> {

    @Override
    public ReactiveKafkaProducer<String, CloudEvent> create(Vertx v, Properties config) {
        return new MockReactiveKafkaProducer<String, CloudEvent>();
    }

    public static ReactiveKafkaProducer<String, CloudEvent> createStatic(Vertx v,
            Producer<String, CloudEvent> producer) {
        return new MockReactiveKafkaProducer<String, CloudEvent>();
    }
    
    public static ReactiveKafkaProducer<String, CloudEvent> createStatic(Vertx v, Properties config) {
        return new MockReactiveKafkaProducer<String, CloudEvent>();
    }

}
