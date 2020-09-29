package dev.knative.eventing.kafka.broker.core.utils;

import static java.util.Objects.requireNonNull;

import java.util.function.Function;

public class MetricsConfigs {

  public static final String METRICS_PORT = "METRICS_PORT";
  public static final String METRICS_PATH = "METRICS_PATH";
  public static final String METRICS_PUBLISH_QUANTILES = "METRICS_PUBLISH_QUANTILES";

  private final int port;
  private final String path;
  private final boolean publishQuantiles;

  public MetricsConfigs(final Function<String, String> envProvider) {
    this.path = requireNonNull(envProvider.apply(METRICS_PATH));
    this.port = Integer.parseInt(requireNonNull(envProvider.apply(METRICS_PORT)));
    this.publishQuantiles = Boolean.parseBoolean(envProvider.apply(METRICS_PUBLISH_QUANTILES));
  }

  public int getPort() {
    return port;
  }

  public String getPath() {
    return path;
  }

  public boolean isPublishQuantilesEnabled() {
    return publishQuantiles;
  }

  @Override
  public String toString() {
    return "MetricsConfigs{" +
      "port=" + port +
      ", path='" + path + '\'' +
      ", publishQuantiles=" + publishQuantiles +
      '}';
  }
}
