package dev.knative.eventing.kafka.broker.core.utils;

import static java.util.Objects.requireNonNull;

import java.util.function.Function;

public abstract class BaseEnv {

  public static final String PRODUCER_CONFIG_FILE_PATH = "PRODUCER_CONFIG_FILE_PATH";
  public static final String DATA_PLANE_CONFIG_FILE_PATH = "DATA_PLANE_CONFIG_FILE_PATH";

  private final String producerConfigFilePath;
  private final String dataPlaneConfigFilePath;

  public BaseEnv(Function<String, String> envProvider) {
    this.producerConfigFilePath = requireNonNull(envProvider.apply(PRODUCER_CONFIG_FILE_PATH));
    this.dataPlaneConfigFilePath = requireNonNull(envProvider.apply(DATA_PLANE_CONFIG_FILE_PATH));
  }

  public String getProducerConfigFilePath() {
    return producerConfigFilePath;
  }

  public String getDataPlaneConfigFilePath() {
    return dataPlaneConfigFilePath;
  }

  @Override
  public String toString() {
    return "BaseEnv{" +
      "producerConfigFilePath='" + producerConfigFilePath + '\'' +
      ", dataPlaneConfigFilePath='" + dataPlaneConfigFilePath + '\'' +
      '}';
  }
}
