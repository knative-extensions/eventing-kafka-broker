package dev.knative.eventing.kafka.broker.core.utils;

import static java.util.Objects.requireNonNull;

import java.util.function.Function;

public class BaseEnv {

  public static final String PRODUCER_CONFIG_FILE_PATH = "PRODUCER_CONFIG_FILE_PATH";
  public static final String DATA_PLANE_CONFIG_FILE_PATH = "DATA_PLANE_CONFIG_FILE_PATH";

  public static final String METRICS_PORT = "METRICS_PORT";
  public static final String METRICS_PATH = "METRICS_PATH";
  public static final String METRICS_PUBLISH_QUANTILES = "METRICS_PUBLISH_QUANTILES";

  private final String producerConfigFilePath;
  private final String dataPlaneConfigFilePath;

  private final int metricsPort;
  private final String metricsPath;
  private final boolean metricsPublishQuantiles;

  public BaseEnv(Function<String, String> envProvider) {
    this.metricsPath = requireNonNull(envProvider.apply(METRICS_PATH));
    this.metricsPort = Integer.parseInt(requireNonNull(envProvider.apply(METRICS_PORT)));
    this.metricsPublishQuantiles = Boolean.parseBoolean(envProvider.apply(METRICS_PUBLISH_QUANTILES));
    this.producerConfigFilePath = requireNonNull(envProvider.apply(PRODUCER_CONFIG_FILE_PATH));
    this.dataPlaneConfigFilePath = requireNonNull(envProvider.apply(DATA_PLANE_CONFIG_FILE_PATH));
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
