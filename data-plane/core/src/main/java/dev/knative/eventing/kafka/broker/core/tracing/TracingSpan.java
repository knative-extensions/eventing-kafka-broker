package dev.knative.eventing.kafka.broker.core.tracing;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import io.cloudevents.CloudEvent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.attributes.SemanticAttributes;
import io.vertx.core.Vertx;

public class TracingSpan {

  static String ACTIVE_CONTEXT = "tracing.context";
  static String ACTIVE_SPAN = "tracing.span";

  public static final AttributeKey<String> MESSAGING_MESSAGE_SOURCE = stringKey("messaging.message_source");
  public static final AttributeKey<String> MESSAGING_MESSAGE_TYPE = stringKey("messaging.message_source");

  public static void decorateCurrent(final Vertx vertx, final CloudEvent event) {

    final var span = getCurrent(vertx);
    if (span == null) {
      return;
    }

    span.setAttribute(SemanticAttributes.MESSAGING_MESSAGE_ID, event.getId());
    span.setAttribute(MESSAGING_MESSAGE_SOURCE, event.getSource().toString());
    span.setAttribute(MESSAGING_MESSAGE_TYPE, event.getType());
  }

  public static Span getCurrent(final Vertx vertx) {
    final var ctx = vertx.getOrCreateContext();
    if (ctx == null) {
      return null;
    }

    return ctx.getLocal(ACTIVE_SPAN);
  }
}
