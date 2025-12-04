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
import dev.knative.eventing.kafka.broker.core.observability.ObservabilityConfig;
import dev.knative.eventing.kafka.broker.core.observability.metrics.Metrics;
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
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(ObservabilityConfig.MetricsProtocol.NONE, "", ""),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocol() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.OTLP_HTTP,
                                "http://otel-collector.otel-collector.svc:4318/v1/metrics",
                                "60s"),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolEmptyEndpoint() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(ObservabilityConfig.MetricsProtocol.OTLP_HTTP, "", "60s"),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolNullEndpoint() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.OTLP_HTTP, null, "60s"),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolEmptyExportInterval() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.OTLP_HTTP,
                                "http://otel-collector.otel-collector.svc:4318/v1/metrics",
                                ""),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolNullExportInterval() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.OTLP_HTTP,
                                "http://otel-collector.otel-collector.svc:4318/v1/metrics",
                                null),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolAllEmptyValues() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(ObservabilityConfig.MetricsProtocol.OTLP_HTTP, "", ""),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolAllNullValues() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.OTLP_HTTP, null, null),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolWhitespaceEndpoint() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.OTLP_HTTP, "   ", "60s"),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolWhitespaceExportInterval() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.OTLP_HTTP,
                                "http://otel-collector.otel-collector.svc:4318/v1/metrics",
                                "   "),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithOtlpHttpProtocolVariousDurationFormats() {
        String[] durationFormats = {"1m", "30s", "5m", "1h", "100ms", "60s"};

        for (String duration : durationFormats) {
            final var metricsOptions = Metrics.getOptions(
                    new BaseEnv(s -> "1"),
                    new ObservabilityConfig(
                            new ObservabilityConfig.MetricsConfig(
                                    ObservabilityConfig.MetricsProtocol.OTLP_HTTP,
                                    "http://otel-collector.otel-collector.svc:4318/v1/metrics",
                                    duration),
                            new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
            assertThat(metricsOptions.isEnabled())
                    .as("Duration format '%s' should work", duration)
                    .isTrue();
        }
    }

    @Test
    public void getWithOtlpHttpProtocolVariousEndpointFormats() {
        String[] endpoints = {
            "http://localhost:4318/v1/metrics",
            "http://otel-collector:4318/v1/metrics",
            "http://otel-collector.otel-collector.svc:4318/v1/metrics",
            "http://otel-collector.otel-collector.svc.cluster.local:4318/v1/metrics",
            "https://otel-collector:4318/v1/metrics",
            "http://127.0.0.1:4318/v1/metrics"
        };

        for (String endpoint : endpoints) {
            final var metricsOptions = Metrics.getOptions(
                    new BaseEnv(s -> "1"),
                    new ObservabilityConfig(
                            new ObservabilityConfig.MetricsConfig(
                                    ObservabilityConfig.MetricsProtocol.OTLP_HTTP, endpoint, "60s"),
                            new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
            assertThat(metricsOptions.isEnabled())
                    .as("Endpoint '%s' should work", endpoint)
                    .isTrue();
        }
    }

    @Test
    public void getWithOtlpGrpcProtocol() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.OTLP_GRPC, "http://otel-collector:4317", "60s"),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithPrometheusProtocol() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(
                                ObservabilityConfig.MetricsProtocol.PROMETHEUS, "http://localhost:9090/metrics", "60s"),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
        assertThat(metricsOptions.isEnabled()).isTrue();
    }

    @Test
    public void getWithPrometheusProtocolEmptyValues() {
        final var metricsOptions = Metrics.getOptions(
                new BaseEnv(s -> "1"),
                new ObservabilityConfig(
                        new ObservabilityConfig.MetricsConfig(ObservabilityConfig.MetricsProtocol.PROMETHEUS, "", ""),
                        new ObservabilityConfig.TracingConfig(ObservabilityConfig.TracingProtocol.NONE, "", 0F)));
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

        Metrics.eventDispatchLatency(
                        Tags.of(Tag.of(Metrics.Tags.EVENT_TYPE, "type"), Tag.of(Metrics.Tags.CONSUMER_NAME, "cname"))
                                .and(Metrics.resourceRefTags(DataPlaneContract.Reference.newBuilder()
                                        .setKind("kind")
                                        .setName("cname")
                                        .setNamespace("namespace")
                                        .build())))
                .register(registry)
                .record(10);

        Metrics.eventProcessingLatency(
                        Tags.of(Tag.of(Metrics.Tags.EVENT_TYPE, "type"), Tag.of(Metrics.Tags.CONSUMER_NAME, "cname"))
                                .and(Metrics.resourceRefTags(DataPlaneContract.Reference.newBuilder()
                                        .setKind("kind")
                                        .setName("cname")
                                        .setNamespace("namespace")
                                        .build())))
                .register(registry)
                .record(10);

        Metrics.eventDispatchLatency(
                        Tags.of(Tag.of(Metrics.Tags.EVENT_TYPE, "type"), Tag.of(Metrics.Tags.CONSUMER_NAME, "cname"))
                                .and(Metrics.resourceRefTags(DataPlaneContract.Reference.newBuilder()
                                        .setKind("kind")
                                        .setName("cname")
                                        .setNamespace("namespace")
                                        .build())))
                .register(registry)
                .record(10);

        final var expectedResourceMetersCount = 2;
        final var expectedEgressMetersCount = 2;

        final var resourceMeters = Metrics.searchResourceMeters(
                registry,
                DataPlaneContract.Reference.newBuilder()
                        .setName("cname")
                        .setNamespace("namespace")
                        .setKind("kind")
                        .build());

        final var egressMeters = Metrics.searchEgressMeters(
                registry,
                DataPlaneContract.Reference.newBuilder()
                        .setName("cname")
                        .setNamespace("namespace")
                        .setKind("kind")
                        .build());

        Assertions.assertThat(resourceMeters).hasSize(expectedResourceMetersCount);
        Assertions.assertThat(egressMeters).hasSize(expectedEgressMetersCount);

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
