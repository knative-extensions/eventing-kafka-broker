package dev.knative.eventing.kafka.broker.core.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;

public class HeadersPropagatorSetterTest {

  @Test
  public void shouldCallConsumerWhenCarrierIsNotNull() {

    final var nCalls = new AtomicInteger(0);
    final BiConsumer<String, String> c = (k, v) -> {
      nCalls.incrementAndGet();

      assertThat(k).isEqualTo("k");
      assertThat(v).isEqualTo("v");
    };

    final var setter = new HeadersPropagatorSetter();

    setter.set(c, "k", "v");

    assertThat(nCalls.get()).isEqualTo(1);
  }

  @Test
  public void shouldNotThrowWhenCarrierIsNull() {
    assertThatNoException().isThrownBy(() -> new HeadersPropagatorSetter().set(null, "k", "v"));
  }
}
