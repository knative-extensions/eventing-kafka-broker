package dev.knative.eventing.kafka.broker.receiver;

import static java.util.Objects.requireNonNull;

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

  Env(final Function<String, String> envProvider) {
    this.ingressPort = Integer.parseInt(envProvider.apply(INGRESS_PORT));
    this.producerConfigFilePath = requireNonNull(envProvider.apply(PRODUCER_CONFIG_FILE_PATH));
    this.livenessProbePath = requireNonNull(envProvider.apply(LIVENESS_PROBE_PATH));
    this.readinessProbePath = requireNonNull(envProvider.apply(READINESS_PROBE_PATH));
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

  @Override
  public String toString() {
    return "Env{"
        + "ingressPort=" + ingressPort
        + ", producerConfigFilePath='" + producerConfigFilePath + '\''
        + ", livenessProbePath='" + livenessProbePath + '\''
        + ", readinessProbePath='" + readinessProbePath + '\''
        + '}';
  }
}
