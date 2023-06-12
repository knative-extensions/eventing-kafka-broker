package dev.knative.eventing.kafka.broker.receiver;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.cloudevents.CloudEvent;
import io.vertx.core.Future;

public class MockReactiveKafkaProducer<K, V> implements ReactiveKafkaProducer<String, CloudEvent> {


    public MockReactiveKafkaProducer() {
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> flush() {
        return Future.succeededFuture();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, CloudEvent> record) {
        return Future.succeededFuture();
    }

    @Override
    public org.apache.kafka.clients.producer.Producer<String, CloudEvent> unwrap() {
        return new org.apache.kafka.clients.producer.MockProducer<String, CloudEvent>();
    }
    
}
