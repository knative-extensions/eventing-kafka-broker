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
package dev.knative.eventing.kafka.broker.receiver;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import org.junit.jupiter.api.Test;

class ReceiverEnvTest {

  private static final String PORT = "8080";
  private static final String LIVENESS_PATH = "/healthz";
  private static final String READINESS_PATH = "/readyz";
  private static final String PRODUCER_CONFIG_PATH = "/etc/producer";
  private static final String DATA_PLANE_CONFIG_FILE_PATH = "/etc/brokers";
  private static final String HTTPSERVER_CONFIG_FILE_PATH = "/etc/http-server-config";
  private static final String TRACING_CONFIG_PATH = "/etc/tracing";
  private static final String METRICS_JVM_ENABLED = "true";

  @Test
  public void create() {
    final var env = new ReceiverEnv(
      key -> switch (key) {
        case ReceiverEnv.INGRESS_PORT -> PORT;
        case ReceiverEnv.LIVENESS_PROBE_PATH -> LIVENESS_PATH;
        case ReceiverEnv.READINESS_PROBE_PATH -> READINESS_PATH;
        case ReceiverEnv.HTTPSERVER_CONFIG_FILE_PATH -> HTTPSERVER_CONFIG_FILE_PATH;
        case BaseEnv.PRODUCER_CONFIG_FILE_PATH -> PRODUCER_CONFIG_PATH;
        case BaseEnv.DATA_PLANE_CONFIG_FILE_PATH -> DATA_PLANE_CONFIG_FILE_PATH;
        case BaseEnv.METRICS_PORT -> "9092";
        case BaseEnv.METRICS_PATH -> "/path";
        case BaseEnv.METRICS_PUBLISH_QUANTILES -> "TRUE";
        case BaseEnv.CONFIG_TRACING_PATH -> TRACING_CONFIG_PATH;
        case BaseEnv.METRICS_JVM_ENABLED -> METRICS_JVM_ENABLED;
        default -> throw new IllegalArgumentException(key);
      }
    );

    assertThat(env.getIngressPort()).isEqualTo(Integer.parseInt(PORT));
    assertThat(env.getLivenessProbePath()).isEqualTo(LIVENESS_PATH);
    assertThat(env.getReadinessProbePath()).isEqualTo(READINESS_PATH);
    assertThat(env.getProducerConfigFilePath()).isEqualTo(PRODUCER_CONFIG_PATH);
    assertThat(env.getDataPlaneConfigFilePath()).isEqualTo(DATA_PLANE_CONFIG_FILE_PATH);
    assertThat(env.getHttpServerConfigFilePath()).isEqualTo(HTTPSERVER_CONFIG_FILE_PATH);
    assertThat(env.getConfigTracingPath()).isEqualTo(TRACING_CONFIG_PATH);
    assertThat(env.isMetricsJvmEnabled()).isEqualTo(Boolean.valueOf(METRICS_JVM_ENABLED));

    // Check toString is overridden
    assertThat(env.toString()).doesNotContain("@");
  }
}
