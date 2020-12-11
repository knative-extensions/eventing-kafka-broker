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
package dev.knative.eventing.kafka.broker.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class BaseEnvTest {


  private static final Function<String, String> provider = s -> switch (s) {
    case "PRODUCER_CONFIG_FILE_PATH" -> "/tmp/config";
    case "DATA_PLANE_CONFIG_FILE_PATH" -> "/tmp/config-data";
    case "METRICS_PORT" -> "9092";
    case "METRICS_PATH" -> "/path";
    case "METRICS_PUBLISH_QUANTILES" -> "TRUE";
    case "CONFIG_TRACING_PATH" -> "/etc/tracing";
    case "METRICS_JVM_ENABLED" -> "false";
    default -> throw new IllegalArgumentException("unknown " + s);
  };

  @Test
  public void shouldGetMetricsPort() {
    final var metricsConfigs = new BaseEnv(provider);
    final var port = metricsConfigs.getMetricsPort();
    assertThat(port).isEqualTo(9092);
  }

  @Test
  public void shouldGetMetricsPath() {
    final var metricsConfigs = new BaseEnv(provider);
    final var path = metricsConfigs.getMetricsPath();
    assertThat(path).isEqualTo("/path");
  }

  @Test
  public void shouldGetIsPublishQuantiles() {
    final var metricsConfigs = new BaseEnv(provider);
    final var isPublishQuantiles = metricsConfigs.isPublishQuantilesEnabled();
    assertThat(isPublishQuantiles).isEqualTo(true);
  }

  @Test
  public void shouldGetConfigTracingPath() {
    final var metricsConfigs = new BaseEnv(provider);
    final var configTracingPath = metricsConfigs.getConfigTracingPath();
    assertThat(configTracingPath).isEqualTo("/etc/tracing");
  }

  @Test
  public void shouldNotPrintAddress() {
    final var metricsConfigs = new BaseEnv(provider);
    assertThat(metricsConfigs.toString()).doesNotContain("@");
  }

  @Test
  public void shouldGetJvmMetricsEnabled() {
    final var metricsConfigs = new BaseEnv(provider);
    assertThat(metricsConfigs.isMetricsJvmEnabled()).isFalse();
  }
}
