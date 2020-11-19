package dev.knative.eventing.kafka.broker.core.tracing;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import io.opentelemetry.context.propagation.TextMapPropagator.Setter;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeadersPropagatorSetter implements Setter<BiConsumer<String, String>> {

  private static final Logger logger = LoggerFactory.getLogger(HeadersPropagatorSetter.class);

  @Override
  public void set(final BiConsumer<String, String> carrier, @Nonnull final String key, @Nonnull final String value) {
    if (carrier == null) {
      return;
    }

    logger.debug("Set {} {}", keyValue("key", key), keyValue("value", value));

    carrier.accept(key, value);
  }
}
