package dev.knative.eventing.kafka.broker.core.tracing;

import io.opentelemetry.context.propagation.TextMapPropagator.Setter;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;

public class HttpHeadersPropagatorSetter implements Setter<BiConsumer<String, String>> {

  @Override
  public void set(final BiConsumer<String, String> carrier, @Nonnull final String key, @Nonnull final String value) {
    if (carrier == null) {
      return;
    }

    carrier.accept(key, value);
  }
}
