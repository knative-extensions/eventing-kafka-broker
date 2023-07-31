/*
 * Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.knative.eventing.kafka.broker.receiverloom;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import io.vertx.core.Vertx;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
