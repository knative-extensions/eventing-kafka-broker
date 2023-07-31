package dev.knative.eventing.kafka.broker.receiverloom;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import io.vertx.core.Vertx;

public class LoomProducerFactoryTest {
    private LoomProducerFactory<String, Integer> factory;
    private Vertx vertx;

    @BeforeEach
    public void setUp() {
        factory = new LoomProducerFactory<>();
        vertx = Vertx.vertx();
    }

    @Test
    public void testCreate() {
        // Prepare the properties for the KafkaProducer
        Properties config = new Properties();
        config.setProperty("bootstrap.servers", "localhost:9092");
        config.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        // Create the ReactiveKafkaProducer using the factory
        ReactiveKafkaProducer<String, Integer> producer = factory.create(vertx, config);

        // Verify that the producer is not null
        assertNotNull(producer);
        Producer<String, Integer> underlyingProducer = producer.unwrap();
        assertNotNull(underlyingProducer);
    }
}
