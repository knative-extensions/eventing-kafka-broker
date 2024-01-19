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
package dev.knative.eventing.kafka.broker.dispatcher.integration;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressContext;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerVerticleFactory;
import dev.knative.eventing.kafka.broker.dispatcher.MockReactiveKafkaConsumer;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleBuilder;
import dev.knative.eventing.kafka.broker.dispatcher.main.FakeConsumerVerticleContext;
import dev.knative.eventing.kafka.broker.receiver.MockReactiveKafkaProducer;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

public class ConsumerVerticleFactoryImplMock implements ConsumerVerticleFactory {

    // trigger.id() -> Mock*er
    private final Map<String, MockProducer<String, CloudEvent>> mockProducer;
    private final Map<String, MockConsumer<String, CloudEvent>> mockConsumer;

    private List<ConsumerRecord<String, CloudEvent>> records;

    public ConsumerVerticleFactoryImplMock() {
        mockProducer = new ConcurrentHashMap<>();
        mockConsumer = new ConcurrentHashMap<>();
    }

    private ReactiveKafkaProducer<String, CloudEvent> createProducer(Vertx vertx, Properties producerConfigs) {
        return new MockReactiveKafkaProducer<>(new MockProducer<>(
                true,
                new StringSerializer(),
                (topic, data) -> new byte[0] // No need to use the real one, since it doesn't support headers
                ));
    }

    /**
     * @param vertx
     * @param consumerConfigs
     * @return
     */
    private ReactiveKafkaConsumer<Object, CloudEvent> createConsumer(Vertx vertx, Map<String, Object> consumerConfigs) {
        final var consumer = new MockConsumer<Object, CloudEvent>(OffsetResetStrategy.LATEST);

        consumer.schedulePollTask(() -> {
            consumer.unsubscribe();

            consumer.assign(records.stream()
                    .map(r -> new TopicPartition(r.topic(), r.partition()))
                    .distinct()
                    .collect(Collectors.toList()));

            records.forEach(record -> consumer.addRecord(new ConsumerRecord<>(
                    record.topic(), record.partition(), record.offset(), record.key(), record.value())));

            consumer.updateEndOffsets(records.stream()
                    .map(r -> new TopicPartition(r.topic(), r.partition()))
                    .distinct()
                    .collect(Collectors.toMap(Function.identity(), v -> 0L)));
        });

        return new MockReactiveKafkaConsumer<Object, CloudEvent>(consumer);
    }

    public void setRecords(final List<ConsumerRecord<String, CloudEvent>> records) {
        this.records = records;
    }

    Map<String, MockProducer<String, CloudEvent>> producers() {
        return mockProducer;
    }

    Map<String, MockConsumer<String, CloudEvent>> consumers() {
        return mockConsumer;
    }

    @Override
    public AbstractVerticle get(final EgressContext egressContext) {
        return new ConsumerVerticleBuilder(
                        FakeConsumerVerticleContext.get(egressContext.resource(), egressContext.egress())
                                .withConsumerFactory(this::createConsumer)
                                .withProducerFactory(this::createProducer))
                .build();
    }
}
