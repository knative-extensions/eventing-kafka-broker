package dev.knative.eventing.kafka.broker.core.tracing;

import static org.assertj.core.api.Assertions.assertThatNoException;

import org.junit.jupiter.api.Test;

public class TracingTest {

  @Test
  public void test() {
    assertThatNoException().isThrownBy(() -> Tracing.setup(TracingConfig.builder().createTracingConfig()));
  }
}
