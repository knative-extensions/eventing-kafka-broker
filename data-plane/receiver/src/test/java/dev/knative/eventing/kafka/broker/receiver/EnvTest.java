package dev.knative.eventing.kafka.broker.receiver;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EnvTest {

  private static final String PORT = "8080";
  private static final String LIVENESS_PATH = "/healthz";
  private static final String READINESS_PATH = "/readyz";
  private static final String PRODUCER_CONFIG_PATH = "/etc/producer";

  @Test
  public void create() {
    final var env = new Env(
        key -> {
          switch (key) {
            case Env.INGRESS_PORT:
              return PORT;
            case Env.LIVENESS_PROBE_PATH:
              return LIVENESS_PATH;
            case Env.READINESS_PROBE_PATH:
              return READINESS_PATH;
            case Env.PRODUCER_CONFIG_FILE_PATH:
              return PRODUCER_CONFIG_PATH;
            default:
              throw new IllegalArgumentException();
          }
        }
    );

    assertThat(env.getIngressPort()).isEqualTo(Integer.parseInt(PORT));
    assertThat(env.getLivenessProbePath()).isEqualTo(LIVENESS_PATH);
    assertThat(env.getReadinessProbePath()).isEqualTo(READINESS_PATH);
    assertThat(env.getProducerConfigFilePath()).isEqualTo(PRODUCER_CONFIG_PATH);
    assertThat(env.toString()).doesNotContain("@");
  }
}