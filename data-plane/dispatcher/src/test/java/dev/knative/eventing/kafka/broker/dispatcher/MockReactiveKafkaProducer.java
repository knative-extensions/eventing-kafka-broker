package dev.knative.eventing.kafka.broker.dispatcher;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class MockReactiveKafkaProducer<K, V> implements ReactiveKafkaProducer<K, V>{

    Producer<K, V> producer;

    public MockReactiveKafkaProducer() {
    }

    public MockReactiveKafkaProducer(Vertx v, Producer<K, V> producer) {
        this.producer = producer;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        final Promise<RecordMetadata> p = Promise.promise();
        producer.send(record, (recordMetadata, exception) -> {
            if (exception != null) {
                p.fail(exception);
            } else {
                p.complete(recordMetadata);
            }
        });
        return p.future();
    }

    @Override
    public Future<Void> close() {
        producer.close();
        return Future.succeededFuture();
    }

    @Override
    public Producer<K, V> unwrap() {
        return producer;
    }
    
}
