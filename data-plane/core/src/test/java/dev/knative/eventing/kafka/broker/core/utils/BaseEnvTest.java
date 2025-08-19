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
        case "CONFIG_OBSERVABILITY_PATH" -> "/etc/observability";
        case "DATA_PLANE_CONFIG_FILE_PATH" -> "/tmp/config-data";
        case "METRICS_PUBLISH_QUANTILES" -> "TRUE";
        case "CONFIG_TRACING_PATH" -> "/etc/tracing";
        case "METRICS_JVM_ENABLED" -> "false";
        case "WAIT_STARTUP_SECONDS" -> "1";
        default -> null;
    };

    @Test
    public void shouldGetIsPublishQuantiles() {
        final var metricsConfigs = new BaseEnv(provider);
        final var isPublishQuantiles = metricsConfigs.isPublishQuantilesEnabled();
        assertThat(isPublishQuantiles).isEqualTo(true);
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

    @Test
    public void shouldGetHttpClientMetricsDisabled() {
        final var metricsConfigs = new BaseEnv(provider);
        assertThat(metricsConfigs.isMetricsHTTPClientEnabled()).isFalse();
    }

    @Test
    public void shouldGetHttpServerMetricsDisabled() {
        final var metricsConfigs = new BaseEnv(provider);
        assertThat(metricsConfigs.isMetricsHTTPServerEnabled()).isFalse();
    }

    @Test
    public void shouldGetObservabilityConfig() {
        final var metricsConfigs = new BaseEnv(provider);
        assertThat(metricsConfigs.getConfigObservabilityPath()).isEqualTo("/etc/observability");
    }
}
