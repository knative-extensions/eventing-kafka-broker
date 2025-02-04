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
package dev.knative.eventing.kafka.broker.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.vertx.junit5.VertxExtension;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class MetricsTest {

    static {
        BackendRegistries.setupBackend(
                new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME), null);
    }

    @Test
    public void get() {
        final var metricsOptions = Metrics.getOptions(new BaseEnv(s -> "1"));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void shouldRegisterAndCloseProducerMetricsThread() throws Exception {
        final var meterBinder = Metrics.register(new MockProducer<>());
        meterBinder.close();
    }

    @Test
    public void shouldRegisterAndCloseConsumerMetricsThread() throws Exception {
        final var meterBinder = Metrics.register(new MockConsumer<>(OffsetResetStrategy.LATEST));
        meterBinder.close();
    }

    @Test
    public void searchShouldReturnMetersWithAdditionalTags() {

        final var registry = Metrics.getRegistry();

        Metrics.eventCount(Tags.of(
                        Tag.of(Metrics.Tags.EVENT_TYPE, "type"),
                        Tag.of(Metrics.Tags.RESOURCE_NAME, "name"),
                        Tag.of(Metrics.Tags.RESOURCE_NAMESPACE, "namespace")))
                .register(registry)
                .increment();

        Metrics.eventDispatchLatency(Tags.of(
                        Tag.of(Metrics.Tags.EVENT_TYPE, "type"),
                        Tag.of(Metrics.Tags.RESOURCE_NAME, "name"),
                        Tag.of(Metrics.Tags.CONSUMER_NAME, "cname"),
                        Tag.of(Metrics.Tags.RESOURCE_NAMESPACE, "namespace")))
                .register(registry)
                .record(10);

        Metrics.eventProcessingLatency(Tags.of(
                        Tag.of(Metrics.Tags.EVENT_TYPE, "type"),
                        Tag.of(Metrics.Tags.RESOURCE_NAME, "name"),
                        Tag.of(Metrics.Tags.CONSUMER_NAME, "cname"),
                        Tag.of(Metrics.Tags.RESOURCE_NAMESPACE, "namespace")))
                .register(registry)
                .record(10);

        Metrics.eventDispatchLatency(Tags.of(
                        Tag.of(Metrics.Tags.EVENT_TYPE, "type"),
                        Tag.of(Metrics.Tags.RESOURCE_NAME, "name"),
                        Tag.of(Metrics.Tags.CONSUMER_NAME, "cname"),
                        Tag.of(Metrics.Tags.RESOURCE_NAMESPACE, "other-namespace")))
                .register(registry)
                .record(10);

        final var expectedResourceMetersCount = 3;
        final var expectedEgressMetersCount = 2;

        final var resourceMeters = Metrics.searchResourceMeters(
                registry,
                DataPlaneContract.Reference.newBuilder()
                        .setName("name")
                        .setNamespace("namespace")
                        .build());

        final var egressMeters = Metrics.searchEgressMeters(
                registry,
                DataPlaneContract.Reference.newBuilder()
                        .setName("cname")
                        .setNamespace("namespace")
                        .build());

        Assertions.assertThat(resourceMeters).hasSize(expectedResourceMetersCount);
        Assertions.assertThat(egressMeters).hasSize(expectedEgressMetersCount);

        Assertions.assertThat(resourceMeters.stream()
                        .filter(m -> m.getId().getName().equals(Metrics.EVENTS_COUNT))
                        .collect(Collectors.toList()))
                .hasSize(1);
        Assertions.assertThat(resourceMeters.stream()
                        .filter(m -> m.getId().getName().equals(Metrics.EVENT_DISPATCH_LATENCY))
                        .collect(Collectors.toList()))
                .hasSize(1);
        Assertions.assertThat(resourceMeters.stream()
                        .filter(m -> m.getId().getName().equals(Metrics.EVENT_PROCESSING_LATENCY))
                        .collect(Collectors.toList()))
                .hasSize(1);

        Assertions.assertThat(egressMeters.stream()
                        .filter(m -> m.getId().getName().equals(Metrics.EVENT_DISPATCH_LATENCY))
                        .collect(Collectors.toList()))
                .hasSize(1);
        Assertions.assertThat(egressMeters.stream()
                        .filter(m -> m.getId().getName().equals(Metrics.EVENT_PROCESSING_LATENCY))
                        .collect(Collectors.toList()))
                .hasSize(1);
    }
}
