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

import static java.util.Objects.requireNonNull;

import dev.knative.eventing.kafka.broker.core.file.FileWatcher.FileFormat;
import java.util.function.Function;

class Env {

  static final String INGRESS_PORT = "INGRESS_PORT";
  private final int ingressPort;

  static final String PRODUCER_CONFIG_FILE_PATH = "PRODUCER_CONFIG_FILE_PATH";
  private final String producerConfigFilePath;

  static final String LIVENESS_PROBE_PATH = "LIVENESS_PROBE_PATH";
  private final String livenessProbePath;

  static final String READINESS_PROBE_PATH = "READINESS_PROBE_PATH";
  private final String readinessProbePath;

  static final String DATA_PLANE_CONFIG_FILE_PATH = "DATA_PLANE_CONFIG_FILE_PATH";
  private final String dataPlaneConfigFilePath;
  static final String DATA_PLANE_CONFIG_FORMAT = "DATA_PLANE_CONFIG_FORMAT";
  private final FileFormat dataPlaneConfigFileFormat;

  Env(final Function<String, String> envProvider) {
    this.ingressPort = Integer.parseInt(envProvider.apply(INGRESS_PORT));
    this.producerConfigFilePath = requireNonNull(envProvider.apply(PRODUCER_CONFIG_FILE_PATH));
    this.livenessProbePath = requireNonNull(envProvider.apply(LIVENESS_PROBE_PATH));
    this.readinessProbePath = requireNonNull(envProvider.apply(READINESS_PROBE_PATH));
    this.dataPlaneConfigFilePath = requireNonNull(envProvider.apply(DATA_PLANE_CONFIG_FILE_PATH));
    final var format = requireNonNull(envProvider.apply(DATA_PLANE_CONFIG_FORMAT));
    this.dataPlaneConfigFileFormat = FileFormat.from(format);
  }

  public int getIngressPort() {
    return ingressPort;
  }

  public String getProducerConfigFilePath() {
    return producerConfigFilePath;
  }

  public String getLivenessProbePath() {
    return livenessProbePath;
  }

  public String getReadinessProbePath() {
    return readinessProbePath;
  }

  public String getDataPlaneConfigFilePath() {
    return dataPlaneConfigFilePath;
  }

  public FileFormat getDataPlaneConfigFileFormat() {
    return dataPlaneConfigFileFormat;
  }

  @Override
  public String toString() {
    return "Env{"
        + "ingressPort=" + ingressPort
        + ", producerConfigFilePath='" + producerConfigFilePath + '\''
        + ", livenessProbePath='" + livenessProbePath + '\''
        + ", readinessProbePath='" + readinessProbePath + '\''
        + ", dataPlaneConfigFilePath='" + dataPlaneConfigFilePath + '\''
        + '}';
  }
}
