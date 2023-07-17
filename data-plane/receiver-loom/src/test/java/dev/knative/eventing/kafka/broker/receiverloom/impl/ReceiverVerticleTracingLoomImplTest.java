package dev.knative.eventing.kafka.broker.receiverloom.impl;

import org.apache.kafka.clients.producer.Producer;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.receiver.impl.ReceiverVerticleTracingTest;
import dev.knative.eventing.kafka.broker.receiverloom.LoomKafkaProducer;
import io.cloudevents.CloudEvent;
import io.vertx.core.Vertx;

public class ReceiverVerticleTracingLoomImplTest extends ReceiverVerticleTracingTest {

    @Override
    public ReactiveKafkaProducer<String, CloudEvent> createKafkaProducer(Vertx vertx,
            Producer<String, CloudEvent> producer) {
        return new LoomKafkaProducer<>(producer);
    }
    
}
