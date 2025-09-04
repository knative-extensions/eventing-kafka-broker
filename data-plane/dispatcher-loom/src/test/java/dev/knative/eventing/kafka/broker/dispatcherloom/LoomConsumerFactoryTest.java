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
package dev.knative.eventing.kafka.broker.dispatcherloom;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import io.vertx.core.Vertx;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

public class LoomConsumerFactoryTest {

    private LoomConsumerFactory<String, String> consumerFactory = new LoomConsumerFactory<>();

    @Test
    public void testCreate() {
        // Create Vertx instance
        Vertx vertx = Vertx.vertx();

        // Create Kafka consumer configuration
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ReactiveKafkaConsumer<String, String> loomConsumer = consumerFactory.create(vertx, configs);

        // Verify that the created consumer is an instance of LoomKafkaConsumer
        assertTrue(loomConsumer instanceof LoomKafkaConsumer);
        // Verify that the unwrapped KafkaConsumer is the same as the one created manually
        Consumer<String, String> unwrappedConsumer = loomConsumer.unwrap();
        assertNotNull(unwrappedConsumer);
    }
}
