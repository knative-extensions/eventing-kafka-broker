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
package dev.knative.eventing.kafka.broker.dispatcher.main;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.BackoffPolicy;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.EgressConfig;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeCreator;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeListerFactory;
import dev.knative.eventing.kafka.broker.core.observability.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressContext;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.dispatcher.MockReactiveConsumerFactory;
import dev.knative.eventing.kafka.broker.receiver.MockReactiveProducerFactory;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.cloudevents.kafka.PartitionKeyExtensionInterceptor;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

public class ConsumerVerticleFactoryImplTest {

    static {
        BackendRegistries.setupBackend(
                new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME), null);
    }

    @Test
    public void shouldAlwaysSucceed() {

        final var consumerProperties = new Properties();
        consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());
        consumerProperties.setProperty(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());

        final var producerConfigs = new Properties();
        producerConfigs.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        producerConfigs.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());
        producerConfigs.setProperty(INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());

        final var verticleFactory = new ConsumerVerticleFactoryImpl(
                consumerProperties,
                new WebClientOptions(),
                producerConfigs,
                mock(AuthProvider.class),
                mock(MeterRegistry.class),
                new MockReactiveConsumerFactory<>(),
                new dev.knative.eventing.kafka.broker.receiver.MockReactiveProducerFactory<>(),
                mock(EventTypeCreator.class),
                mock(EventTypeListerFactory.class));

        final var egress = DataPlaneContract.Egress.newBuilder()
                .setConsumerGroup("1234")
                .setUid("1234")
                .setDestination("http://localhost:43256")
                .setReplyToOriginalTopic(DataPlaneContract.Empty.newBuilder().build())
                .build();
        final var resource = DataPlaneContract.Resource.newBuilder()
                .setUid("123456")
                .setBootstrapServers("0.0.0.0:9092")
                .addTopics("t1")
                .setEgressConfig(DataPlaneContract.EgressConfig.newBuilder()
                        .setBackoffDelay(1000)
                        .setBackoffPolicy(BackoffPolicy.Exponential)
                        .setRetry(10)
                        .setDeadLetter("http://localhost:43257"))
                .addEgresses(egress)
                .build();

        assertDoesNotThrow(() -> verticleFactory.get(new EgressContext(resource, egress, Collections.emptySet())));
    }

    @Test
    public void shouldAlwaysSucceedWhenPassingResourceReference() {

        final var consumerProperties = new Properties();
        consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());
        consumerProperties.setProperty(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());

        final var producerConfigs = new Properties();
        producerConfigs.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        producerConfigs.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());
        producerConfigs.setProperty(INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());

        final var verticleFactory = new ConsumerVerticleFactoryImpl(
                consumerProperties,
                new WebClientOptions(),
                producerConfigs,
                mock(AuthProvider.class),
                mock(MeterRegistry.class),
                new MockReactiveConsumerFactory<>(),
                new MockReactiveProducerFactory<>(),
                mock(EventTypeCreator.class),
                mock(EventTypeListerFactory.class));

        final var egress = DataPlaneContract.Egress.newBuilder()
                .setConsumerGroup("1234")
                .setUid("1234")
                .setDestination("http://localhost:43256")
                .setReplyToOriginalTopic(DataPlaneContract.Empty.newBuilder().build())
                .build();
        final var resource = DataPlaneContract.Resource.newBuilder()
                .setUid("123456")
                .setBootstrapServers("0.0.0.0:9092")
                .addTopics("t1")
                .setReference(DataPlaneContract.Reference.newBuilder()
                        .setName("name")
                        .setNamespace("ns")
                        .build())
                .setEgressConfig(EgressConfig.newBuilder()
                        .setBackoffDelay(1000)
                        .setBackoffPolicy(BackoffPolicy.Exponential)
                        .setRetry(10)
                        .setDeadLetter("http://localhost:43257"))
                .addEgresses(egress)
                .build();

        assertDoesNotThrow(() -> verticleFactory.get(new EgressContext(resource, egress, Collections.emptySet())));
    }

    @Test
    public void shouldNotThrowIllegalArgumentExceptionIfNotDeadLetterSink() {

        final var consumerProperties = new Properties();
        consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());
        consumerProperties.setProperty(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());

        final var producerConfigs = new Properties();
        producerConfigs.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        producerConfigs.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());
        producerConfigs.setProperty(INTERCEPTOR_CLASSES_CONFIG, PartitionKeyExtensionInterceptor.class.getName());

        final var verticleFactory = new ConsumerVerticleFactoryImpl(
                consumerProperties,
                new WebClientOptions(),
                producerConfigs,
                mock(AuthProvider.class),
                mock(MeterRegistry.class),
                new MockReactiveConsumerFactory<>(),
                new MockReactiveProducerFactory<>(),
                mock(EventTypeCreator.class),
                mock(EventTypeListerFactory.class));

        final var egress = DataPlaneContract.Egress.newBuilder()
                .setConsumerGroup("1234")
                .setUid("1234")
                .setDestination("http://localhost:43256")
                .setReplyToOriginalTopic(DataPlaneContract.Empty.newBuilder().build())
                .build();
        final var resource = DataPlaneContract.Resource.newBuilder()
                .setUid("123456")
                .setBootstrapServers("0.0.0.0:9092")
                .addTopics("t1")
                .addEgresses(egress)
                .build();

        assertDoesNotThrow(() -> verticleFactory.get(new EgressContext(resource, egress, Collections.emptySet())));
    }
}
