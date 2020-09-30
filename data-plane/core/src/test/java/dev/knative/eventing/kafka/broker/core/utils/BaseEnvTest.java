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
  public void shouldNotPrintAddress() {
    final var metricsConfigs = new BaseEnv(provider);
    assertThat(metricsConfigs.toString()).doesNotContain("@");
  }
}
