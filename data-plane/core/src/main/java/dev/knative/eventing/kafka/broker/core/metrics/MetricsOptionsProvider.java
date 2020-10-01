/*
 * Copyright 2020 The Knative Authors
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
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;

public class MetricsOptionsProvider {

  /**
   * Get metrics options from the given metrics configurations.
   *
   * @param metricsConfigs metrics configurations.
   * @param registry       registry name
   * @return metrics options
   */
  public static MetricsOptions get(final BaseEnv metricsConfigs, final String registry) {
    return new MicrometerMetricsOptions()
      .setEnabled(true)
      .setRegistryName(registry)
      .setPrometheusOptions(new VertxPrometheusOptions()
        .setEmbeddedServerOptions(new HttpServerOptions().setPort(metricsConfigs.getMetricsPort()))
        .setEmbeddedServerEndpoint(metricsConfigs.getMetricsPath())
        .setPublishQuantiles(metricsConfigs.isPublishQuantilesEnabled())
        .setStartEmbeddedServer(true)
        .setEnabled(true)
      );
  }
}
