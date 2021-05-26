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

import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.MetricsNaming;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public class Metrics {

  public static final String METRICS_REGISTRY_NAME = "metrics";

  /**
   * Get metrics options from the given metrics configurations.
   *
   * @param metricsConfigs Metrics configurations.
   * @return Metrics options.
   */
  public static MetricsOptions getOptions(final BaseEnv metricsConfigs) {
    return new MicrometerMetricsOptions()
      .setEnabled(true)
      .addDisabledMetricsCategory(MetricsDomain.EVENT_BUS)
      .addDisabledMetricsCategory(MetricsDomain.DATAGRAM_SOCKET)
      // NAMED_POOL allocates a lot, so disable it.
      // See https://github.com/vert-x3/vertx-micrometer-metrics/blob/0646e66de120366c622a7240676d63cb69965ec5/src/main/java/io/vertx/micrometer/impl/meters/Gauges.java#L56-L69
      .addDisabledMetricsCategory(MetricsDomain.NAMED_POOLS)
      .setMetricsNaming(MetricsNaming.v4Names())
      .setRegistryName(METRICS_REGISTRY_NAME)
      .setJvmMetricsEnabled(metricsConfigs.isMetricsJvmEnabled())
      .setPrometheusOptions(new VertxPrometheusOptions()
        .setEmbeddedServerOptions(new HttpServerOptions()
          .setPort(metricsConfigs.getMetricsPort())
          .setTracingPolicy(TracingPolicy.IGNORE)
        )
        .setEmbeddedServerEndpoint(metricsConfigs.getMetricsPath())
        .setPublishQuantiles(metricsConfigs.isPublishQuantilesEnabled())
        .setStartEmbeddedServer(true)
        .setEnabled(true)
      );
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
   * @param <K>      Record key type.
   * @param <V>      Record value type.
   * @return A meter binder to close once the consumer is closed.
   */
  public static <K, V> AutoCloseable register(final Consumer<K, V> consumer) {
    final var clientMetrics = new KafkaClientMetrics(consumer);
    clientMetrics.bindTo(getRegistry());
    return clientMetrics;
  }

  /**
   * Register the given producer to the global meter registry.
   *
   * @param producer Consumer to bind to the global registry.
   * @param <K>      Record key type.
   * @param <V>      Record value type.
   * @return A meter binder to close once the producer is closed.
   */
  public static <K, V> AutoCloseable register(final Producer<K, V> producer) {
    final var clientMetrics = new KafkaClientMetrics(producer);
    clientMetrics.bindTo(getRegistry());
    return clientMetrics;
  }

  /**
   * Close the given meter binder.
   *
   * @param vertx       vertx instance.
   * @param meterBinder meter binder to close.
   * @return A succeeded or a failed future.
   */
  public static Future<?> close(final Vertx vertx, final AutoCloseable meterBinder) {
    return vertx.executeBlocking(promise -> {
      try {
        meterBinder.close();
        promise.complete();
      } catch (Exception e) {
        promise.fail(e);
      }
    });
  }
}
