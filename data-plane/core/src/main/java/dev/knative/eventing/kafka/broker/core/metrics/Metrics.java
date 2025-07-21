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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.MetricsNaming;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics {

    private static final Logger logger = LoggerFactory.getLogger(Metrics.class);

    private static final PemKeyCertOptions pemKeyCertOptions = permKeyCertOptions();
    private static final String host = getHost();

    public static final String METRICS_REGISTRY_NAME = "metrics";

    public static final boolean DISABLE_KAFKA_CLIENTS_METRICS =
            Boolean.parseBoolean(System.getenv("DISABLE_KAFKA_CLIENTS_METRICS"));

    // There are different thread polls usable, mainly, each with its own drawbacks
    // for our use case:
    // - cached thread pools
    // - fixed thread pools
    //
    // A cached thread might grow unbounded and since creating, updating and
    // deleting resources
    // trigger the usage of this executor, a bad actor might start continuously
    // creating, updating
    // and deleting resources which will cause resource exhaustion.
    //
    // A fixed thread poll doesn't give the best possible latency for every
    // resource, but it's
    // bounded, so we keep the resource usage under control.
    // We might want to provide configs to make it bigger than a single thread but a
    // single thread
    // to start with is good enough for now.
    public static final ExecutorService meterBinderExecutor = Executors.newSingleThreadExecutor();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(meterBinderExecutor::shutdown));
    }

    // Micrometer employs a naming convention that separates lowercase words with a
    // '.' (dot) character.
    // Different monitoring systems have different recommendations regarding naming
    // convention, and some naming
    // conventions may be incompatible for one system and not another.
    // Each Micrometer implementation for a monitoring system comes with a naming
    // convention that transforms lowercase
    // dot notation names to the monitoring system’s recommended naming convention.
    // Additionally, this naming convention implementation sanitizes metric names
    // and tags of special characters that
    // are disallowed by the monitoring system.

    /** In prometheus format --> http_events_sent_total */
    public static final String HTTP_EVENTS_SENT_COUNT = "http.events.sent";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     * @see Metrics#eventCount(io.micrometer.core.instrument.Tags)
     */
    public static final String EVENTS_COUNT = "event_count";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     * @see Metrics#eventDispatchLatency(io.micrometer.core.instrument.Tags)
     */
    public static final String EVENT_DISPATCH_LATENCY = "event_dispatch_latencies";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     * @see Metrics#eventDispatchInFlightCount(io.micrometer.core.instrument.Tags, Supplier)
     */
    public static final String EVENT_DISPATCH_IN_FLIGHT_REQUESTS_LATENCY = "event_dispatch_in_flight_count";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     * @see Metrics#eventDispatchLatency(io.micrometer.core.instrument.Tags)
     */
    public static final String EVENT_PROCESSING_LATENCY = "event_processing_latencies";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     * @see Metrics#discardedEventCount(io.micrometer.core.instrument.Tags)
     */
    public static final String DISCARDED_EVENTS_COUNT = "discarded_invalid_event_count";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     * @see Metrics#skippedOffsetCount(io.micrometer.core.instrument.Tags)
     */
    public static final String SKIPPED_OFFSET_COUNT = "skipped_offset_count";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     * @see Metrics#executorQueueLatency(io.micrometer.core.instrument.Tags)
     */
    public static final String EXECUTOR_QUEUE_LATENCY = "executor_queue_latencies";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     * @see Metrics#queueLength(io.micrometer.core.instrument.Tags, Supplier)
     */
    public static final String QUEUE_LENGTH = "queue_length";

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     */
    public static class Tags {
        public static final String RESPONSE_CODE = "response_code";
        public static final String RESPONSE_CODE_CLASS = "response_code_class";
        public static final String EVENT_TYPE = "event_type";

        public static final String RESOURCE_NAME = "name";
        public static final String RESOURCE_NAMESPACE = "namespace_name";
        public static final String CONSUMER_NAME = "consumer_name";
        public static final String SENDER_CONTEXT = "sender_context";
        public static final String PARTITION_ID = "partition";
        public static final String TOPIC_ID = "topic";

        public static io.micrometer.core.instrument.Tags senderContext(final String value) {
            return io.micrometer.core.instrument.Tags.of(SENDER_CONTEXT, value);
        }
    }

    /**
     * @link https://knative.dev/docs/eventing/observability/metrics/eventing-metrics/
     */
    public static class Units {
        // Unified Code for Units of Measure: http://unitsofmeasure.org/ucum.html
        public static final String DIMENSIONLESS = "1";
    }

    private static final double[] LATENCY_SLOs = new double[] {50, 100, 500, 1000, 5000, 10000};

    /**
     * Get metrics options from the given metrics configurations.
     *
     * @param metricsConfigs Metrics configurations.
     * @return Metrics options.
     */
    public static MetricsOptions getOptions(final BaseEnv metricsConfigs) {
        final var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        final var options = new MicrometerMetricsOptions()
                .setEnabled(true)
                .addDisabledMetricsCategory(MetricsDomain.VERTICLES)
                .addDisabledMetricsCategory(MetricsDomain.EVENT_BUS)
                .addDisabledMetricsCategory(MetricsDomain.DATAGRAM_SOCKET)
                // NAMED_POOL allocates a lot, so disable it.
                // See
                // https://github.com/vert-x3/vertx-micrometer-metrics/blob/0646e66de120366c622a7240676d63cb69965ec5/src/main/java/io/vertx/micrometer/impl/meters/Gauges.java#L56-L69
                .addDisabledMetricsCategory(MetricsDomain.NAMED_POOLS)
                .setMetricsNaming(MetricsNaming.v4Names())
                .setRegistryName(METRICS_REGISTRY_NAME)
                .setJvmMetricsEnabled(metricsConfigs.isMetricsJvmEnabled())
                .setMicrometerRegistry(registry)
                .setPrometheusOptions(new VertxPrometheusOptions()
                        .setEmbeddedServerOptions(new HttpServerOptions()
                                .setPort(metricsConfigs.getMetricsPort())
                                .setTracingPolicy(TracingPolicy.IGNORE)
                                .setSsl(pemKeyCertOptions != null)
                                .setPemKeyCertOptions(pemKeyCertOptions)
                                .setHost(host))
                        .setEmbeddedServerEndpoint(metricsConfigs.getMetricsPath())
                        .setPublishQuantiles(metricsConfigs.isPublishQuantilesEnabled())
                        .setStartEmbeddedServer(true)
                        .setEnabled(true));

        if (!metricsConfigs.isMetricsHTTPClientEnabled()) {
            options.addDisabledMetricsCategory(MetricsDomain.HTTP_CLIENT);
            options.addDisabledMetricsCategory(MetricsDomain.NET_CLIENT);
        }
        if (!metricsConfigs.isMetricsHTTPServerEnabled()) {
            options.addDisabledMetricsCategory(MetricsDomain.HTTP_SERVER);
            options.addDisabledMetricsCategory(MetricsDomain.NET_SERVER);
        }

        return options;
    }

    /**
     * @return Global registry.
     */
    public static MeterRegistry getRegistry() {
        return BackendRegistries.getNow(METRICS_REGISTRY_NAME);
    }

    /**
     * Register the given consumer to the global meter registry.
     *
     * @param consumer consumer to bind to the global registry.
     * @param <K> Record key type.
     * @param <V> Record value type.
     * @return A meter binder to close once the consumer is closed.
     */
    public static <K, V> AsyncCloseable register(final Consumer<K, V> consumer) {
        return register(() -> new KafkaClientMetrics(consumer));
    }

    /**
     * Register the given producer to the global meter registry.
     *
     * @param producer Consumer to bind to the global registry.
     * @param <K> Record key type.
     * @param <V> Record value type.
     * @return A meter binder to close once the producer is closed.
     */
    public static <K, V> AsyncCloseable register(final Producer<K, V> producer) {
        return register(() -> new KafkaClientMetrics(producer));
    }

    /**
     * Register the given metrics provider to the global meter registry.
     *
     * @param metricsProvider metrics to bind to the global registry.
     * @return A meter binder to close once the consumer or producer is closed.
     */
    private static AsyncCloseable register(final Supplier<KafkaClientMetrics> metricsProvider) {
        final var registry = getRegistry();
        if (registry != null && !DISABLE_KAFKA_CLIENTS_METRICS) {
            final var clientMetrics = metricsProvider.get();
            try {
                // The binding and close process is blocking, so execute them asynchronously.
                meterBinderExecutor.execute(() -> clientMetrics.bindTo(registry));

                return () -> {
                    final Promise<Void> p = Promise.promise();
                    meterBinderExecutor.execute(() -> {
                        logger.debug("Closing metrics");
                        try {
                            clientMetrics.close();
                            p.complete();
                        } catch (final Exception ex) {
                            p.fail(ex);
                        }
                    });
                    return p.future();
                };

            } catch (final RejectedExecutionException ex) {
                // if this task cannot be accepted for execution when the executor has been
                // shutdown.
                logger.warn("Failed to bind metrics for Kafka client", ex);
            }
        }
        return Future::succeededFuture;
    }

    public static PemKeyCertOptions permKeyCertOptions() {
        final var certPath = System.getenv().get("METRICS_PEM_CERT_PATH");
        final var keyPath = System.getenv().get("METRICS_PEM_KEY_PATH");
        if (certPath == null || keyPath == null) {
            logger.info("Metrics cert paths weren't provided, server will start without TLS");
            return null;
        }
        return new PemKeyCertOptions().setCertPath(certPath).setKeyPath(keyPath);
    }

    private static String getHost() {
        final var host = System.getenv().get("METRICS_HOST");
        if (host == null) {
            logger.info("Metrics server host wasn't provided, using default value " + HttpServerOptions.DEFAULT_HOST);
            return HttpServerOptions.DEFAULT_HOST;
        }
        return host;
    }

    public static Counter.Builder eventCount(final io.micrometer.core.instrument.Tags tags) {
        return Counter.builder(EVENTS_COUNT)
                .description("Number of events received")
                .tags(tags)
                .baseUnit(Metrics.Units.DIMENSIONLESS);
    }

    public static Collection<Meter> searchResourceMeters(
            final MeterRegistry registry, final DataPlaneContract.Reference ref) {
        return Search.in(registry).tags(resourceRefTags(ref)).meters();
    }

    public static Collection<Meter> searchEgressMeters(
            final MeterRegistry registry, final DataPlaneContract.Reference ref) {
        return Search.in(registry).tags(egressRefTags(ref)).meters();
    }

    public static DistributionSummary.Builder eventDispatchLatency(final io.micrometer.core.instrument.Tags tags) {
        return DistributionSummary.builder(EVENT_DISPATCH_LATENCY)
                .description("The time spent dispatching an event to Kafka")
                .tags(tags)
                .baseUnit(BaseUnits.MILLISECONDS)
                .serviceLevelObjectives(LATENCY_SLOs);
    }

    public static Gauge.Builder<Supplier<Number>> eventDispatchInFlightCount(
            final io.micrometer.core.instrument.Tags tags, final Supplier<Number> supplier) {
        return Gauge.builder(EVENT_DISPATCH_IN_FLIGHT_REQUESTS_LATENCY, supplier)
                .description("Number of events currently dispatched to a destination waiting to be processed")
                .tags(tags)
                .baseUnit(Units.DIMENSIONLESS);
    }

    public static DistributionSummary.Builder eventProcessingLatency(final io.micrometer.core.instrument.Tags tags) {
        return DistributionSummary.builder(EVENT_PROCESSING_LATENCY)
                .description("The time spent processing an event")
                .tags(tags)
                .baseUnit(BaseUnits.MILLISECONDS)
                .serviceLevelObjectives(LATENCY_SLOs);
    }

    public static Counter.Builder discardedEventCount(final io.micrometer.core.instrument.Tags tags) {
        return Counter.builder(DISCARDED_EVENTS_COUNT)
                .description("Number of invalid events discarded")
                .tags(tags)
                .baseUnit(Metrics.Units.DIMENSIONLESS);
    }

    public static Counter.Builder skippedOffsetCount(final io.micrometer.core.instrument.Tags tags) {
        return Counter.builder(SKIPPED_OFFSET_COUNT)
                .description("Number of skipped offsets")
                .tags(tags)
                .baseUnit(Metrics.Units.DIMENSIONLESS);
    }

    public static DistributionSummary.Builder executorQueueLatency(final io.micrometer.core.instrument.Tags tags) {
        return DistributionSummary.builder(EXECUTOR_QUEUE_LATENCY)
                .description("The time an event spends in an executor queue")
                .tags(tags)
                .baseUnit(BaseUnits.MILLISECONDS)
                .serviceLevelObjectives(LATENCY_SLOs);
    }

    public static Gauge.Builder<Supplier<Number>> queueLength(
            final io.micrometer.core.instrument.Tags tags, final Supplier<Number> queueSize) {
        return Gauge.builder(QUEUE_LENGTH, queueSize)
                .description("Number of events in executor queue per partition")
                .tags(tags)
                .baseUnit(Metrics.Units.DIMENSIONLESS);
    }

    public static io.micrometer.core.instrument.Tags resourceRefTags(final DataPlaneContract.Reference ref) {
        return io.micrometer.core.instrument.Tags.of(
                Tag.of(Metrics.Tags.RESOURCE_NAME, ref.getName()),
                Tag.of(Metrics.Tags.RESOURCE_NAMESPACE, ref.getNamespace()));
    }

    public static io.micrometer.core.instrument.Tags egressRefTags(final DataPlaneContract.Reference ref) {
        return io.micrometer.core.instrument.Tags.of(
                Tag.of(Metrics.Tags.CONSUMER_NAME, ref.getName()),
                Tag.of(Metrics.Tags.RESOURCE_NAMESPACE, ref.getNamespace()));
    }
}
