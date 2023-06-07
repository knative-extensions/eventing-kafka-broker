package dev.knative.eventing.kafka.broker.receiver;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.clients.producer.Producer;

import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;

public class MockReactiveKafkaProducer<K, V> implements ReactiveKafkaProducer<String, CloudEvent> {

    KafkaProducer<String, CloudEvent> kafkaProducer;

    public MockReactiveKafkaProducer() {
    }

    public MockReactiveKafkaProducer(Vertx v, Producer<String, CloudEvent> producer) {
        kafkaProducer = KafkaProducer.create(v,producer);
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
