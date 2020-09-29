package dev.knative.eventing.kafka.broker.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class MetricsConfigsTest {

  private static final Function<String, String> provider = s -> switch (s) {
    case "METRICS_PORT" -> "9092";
    case "METRICS_PATH" -> "/path";
    case "METRICS_PUBLISH_QUANTILES" -> "TRUE";
    default -> throw new IllegalArgumentException("unknown " + s);
  };

  @Test
  public void shouldGetMetricsPort() {
    final var metricsConfigs = new MetricsConfigs(provider);
    final var port = metricsConfigs.getPort();
    assertThat(port).isEqualTo(9092);
  }

  @Test
  public void shouldGetMetricsPath() {
    final var metricsConfigs = new MetricsConfigs(provider);
    final var path = metricsConfigs.getPath();
    assertThat(path).isEqualTo("/path");
  }

  @Test
  public void shouldGetIsPublishQuantiles() {
    final var metricsConfigs = new MetricsConfigs(provider);
    final var isPublishQuantiles = metricsConfigs.isPublishQuantilesEnabled();
    assertThat(isPublishQuantiles).isEqualTo(true);
  }

  @Test
  public void shouldNotPrintAddress() {
    final var metricsConfigs = new MetricsConfigs(provider);
    assertThat(metricsConfigs.toString()).doesNotContain("@");
  }
}
