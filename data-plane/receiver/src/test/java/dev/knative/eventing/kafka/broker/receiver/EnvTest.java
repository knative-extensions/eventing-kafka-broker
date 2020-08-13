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

package dev.knative.eventing.kafka.broker.receiver;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.file.FileWatcher.FileFormat;
import org.junit.jupiter.api.Test;

class EnvTest {

  private static final String PORT = "8080";
  private static final String LIVENESS_PATH = "/healthz";
  private static final String READINESS_PATH = "/readyz";
  private static final String PRODUCER_CONFIG_PATH = "/etc/producer";
  private static final String DATA_PLANE_CONFIG_FILE_PATH = "/etc/brokers";
  private static final String DATA_PLANE_CONFIG_FILE_FORMAT_PROTOBUF = "protobuf";
  private static final String DATA_PLANE_CONFIG_FILE_FORMAT_JSON = "json";

  @Test
  public void createProtobuf() {
    create(DATA_PLANE_CONFIG_FILE_FORMAT_PROTOBUF, FileFormat.PROTOBUF);
  }

  @Test
  public void createJson() {
    create(DATA_PLANE_CONFIG_FILE_FORMAT_JSON, FileFormat.JSON);
  }

  private static void create(final String format, final FileFormat fileFormat) {
    final var env = new Env(
        key -> switch (key) {
          case Env.INGRESS_PORT -> PORT;
          case Env.LIVENESS_PROBE_PATH -> LIVENESS_PATH;
          case Env.READINESS_PROBE_PATH -> READINESS_PATH;
          case Env.PRODUCER_CONFIG_FILE_PATH -> PRODUCER_CONFIG_PATH;
          case Env.DATA_PLANE_CONFIG_FILE_PATH -> DATA_PLANE_CONFIG_FILE_PATH;
          case Env.DATA_PLANE_CONFIG_FORMAT -> format;
          default -> throw new IllegalArgumentException();
        }
    );

    assertThat(env.getIngressPort()).isEqualTo(Integer.parseInt(PORT));
    assertThat(env.getLivenessProbePath()).isEqualTo(LIVENESS_PATH);
    assertThat(env.getReadinessProbePath()).isEqualTo(READINESS_PATH);
    assertThat(env.getProducerConfigFilePath()).isEqualTo(PRODUCER_CONFIG_PATH);
    assertThat(env.getDataPlaneConfigFilePath()).isEqualTo(DATA_PLANE_CONFIG_FILE_PATH);
    assertThat(env.getDataPlaneConfigFileFormat()).isEqualTo(fileFormat);
    assertThat(env.toString()).doesNotContain("@");
  }
}