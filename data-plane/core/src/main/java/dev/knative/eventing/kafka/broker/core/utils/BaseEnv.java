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

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class BaseEnv {

  public static final String PRODUCER_CONFIG_FILE_PATH = "PRODUCER_CONFIG_FILE_PATH";
  private final String producerConfigFilePath;

  public static final String DATA_PLANE_CONFIG_FILE_PATH = "DATA_PLANE_CONFIG_FILE_PATH";
  private final String dataPlaneConfigFilePath;

  public static final String METRICS_PORT = "METRICS_PORT";
  private final int metricsPort;

  public static final String METRICS_PATH = "METRICS_PATH";
  private final String metricsPath;

  public static final String METRICS_PUBLISH_QUANTILES = "METRICS_PUBLISH_QUANTILES";
  private final boolean metricsPublishQuantiles;

  public static final String METRICS_JVM_ENABLED = "METRICS_JVM_ENABLED";
  private final boolean metricsJvmEnabled;

  public static final String METRICS_HTTP_CLIENT_ENABLED = "METRICS_HTTP_CLIENT_ENABLED";
  private final boolean metricsHTTPClientEnabled;

  public static final String METRICS_HTTP_SERVER_ENABLED = "METRICS_HTTP_SERVER_ENABLED";
  private final boolean metricsHTTPServerEnabled;

  public static final String CONFIG_TRACING_PATH = "CONFIG_TRACING_PATH";
  private final String configTracingPath;

  public static final String WAIT_STARTUP_SECONDS = "WAIT_STARTUP_SECONDS";
  private final int waitStartupSeconds;

  public BaseEnv(Function<String, String> envProvider) {
    this.metricsPath = requireNonNull(envProvider.apply(METRICS_PATH));
    this.metricsPort = Integer.parseInt(requireNonNull(envProvider.apply(METRICS_PORT)));
    this.metricsPublishQuantiles = Boolean.parseBoolean(envProvider.apply(METRICS_PUBLISH_QUANTILES));
    this.metricsJvmEnabled = Boolean.parseBoolean(envProvider.apply(METRICS_JVM_ENABLED));
    this.metricsHTTPClientEnabled = Boolean.parseBoolean(envProvider.apply(METRICS_HTTP_CLIENT_ENABLED));
    this.metricsHTTPServerEnabled = Boolean.parseBoolean(envProvider.apply(METRICS_HTTP_SERVER_ENABLED));
    this.producerConfigFilePath = requireNonNull(envProvider.apply(PRODUCER_CONFIG_FILE_PATH));
    this.dataPlaneConfigFilePath = requireNonNull(envProvider.apply(DATA_PLANE_CONFIG_FILE_PATH));
    this.configTracingPath = requireNonNull(envProvider.apply(CONFIG_TRACING_PATH));
    this.waitStartupSeconds = Integer.parseInt(envProvider.apply(WAIT_STARTUP_SECONDS));
  }

  public String getProducerConfigFilePath() {
    return producerConfigFilePath;
  }

  public String getDataPlaneConfigFilePath() {
    return dataPlaneConfigFilePath;
  }

  public int getMetricsPort() {
    return metricsPort;
  }

  public String getMetricsPath() {
    return metricsPath;
  }

  public boolean isPublishQuantilesEnabled() {
    return metricsPublishQuantiles;
  }

  public boolean isMetricsJvmEnabled() {
    return metricsJvmEnabled;
  }

  public boolean isMetricsHTTPClientEnabled() {
    return metricsHTTPClientEnabled;
  }

  public boolean isMetricsHTTPServerEnabled() {
    return metricsHTTPServerEnabled;
  }

  public String getConfigTracingPath() {
    return configTracingPath;
  }

  public int getWaitStartupSeconds() {
    return waitStartupSeconds;
  }

  @Override
  public String toString() {
    return "BaseEnv{" +
      "producerConfigFilePath='" + producerConfigFilePath + '\'' +
      ", dataPlaneConfigFilePath='" + dataPlaneConfigFilePath + '\'' +
      ", metricsPort=" + metricsPort +
      ", metricsPath='" + metricsPath + '\'' +
      ", metricsPublishQuantiles=" + metricsPublishQuantiles +
      '}';
  }
}
