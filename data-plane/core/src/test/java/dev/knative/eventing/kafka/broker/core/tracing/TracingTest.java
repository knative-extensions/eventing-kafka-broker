package dev.knative.eventing.kafka.broker.core.tracing;

import org.junit.jupiter.api.Test;

public class TracingTest {

  @Test
  public void test() {
    Tracing.setup(TracingConfig.builder().createTracingConfig());
  }
}
