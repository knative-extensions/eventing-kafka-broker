package dev.knative.eventing.kafka.broker.core.tracing;

import static dev.knative.eventing.kafka.broker.core.tracing.TracingSpan.ACTIVE_CONTEXT;
import static dev.knative.eventing.kafka.broker.core.tracing.TracingSpan.ACTIVE_SPAN;
import static io.opentelemetry.context.Context.current;
import static net.logstash.logback.argument.StructuredArguments.keyValue;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Span.Kind;
import io.opentelemetry.api.trace.attributes.SemanticAttributes;
import io.opentelemetry.api.trace.propagation.HttpTraceContext;
import io.opentelemetry.context.propagation.TextMapPropagator.Getter;
import io.opentelemetry.context.propagation.TextMapPropagator.Setter;
import io.vertx.core.Context;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingPolicy;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenTelemetryTracer implements VertxTracer<Span, Span> {

  private static final Logger logger = LoggerFactory.getLogger(OpenTelemetryTracer.class);

  private static final Getter<Iterable<Entry<String, String>>> getter = new HeadersPropagatorGetter();
  private static final Setter<BiConsumer<String, String>> setter = new HeadersPropagatorSetter();

  private final io.opentelemetry.api.trace.Tracer tracer;

  OpenTelemetryTracer(final io.opentelemetry.api.trace.Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public <R> Span receiveRequest(
    final Context context,
    final SpanKind kind,
    final TracingPolicy policy,
    final R request,
    final String operation,
    final Iterable<Entry<String, String>> headers,
    final TagExtractor<R> tagExtractor) {

    if (TracingPolicy.IGNORE.equals(policy)) {
      return null;
    }

    final var parentContext = current();
    final var tracingContext = HttpTraceContext.getInstance().extract(parentContext, headers, getter);

    // OpenTelemetry SDK's Context is immutable, therefore if the extracted context is the same as the parent context
    // there is no tracing data to propagate downstream and we can return null.
    if (tracingContext == parentContext && TracingPolicy.PROPAGATE.equals(policy)) {
      return null;
    }

    final var span = tracer.spanBuilder(operation)
      .setParent(tracingContext)
      .setSpanKind(SpanKind.RPC.equals(kind) ? Kind.SERVER : Kind.CONSUMER)
      .setAttribute(SemanticAttributes.SERVICE_NAME, Tracing.SERVICE_NAME)
      .setAttribute(SemanticAttributes.SERVICE_NAMESPACE, Tracing.SERVICE_NAMESPACE)
      .startSpan();

    logger.debug("{} {} {} {}",
      keyValue("context", tracingContext.getClass()),
      keyValue("span", span.getClass()),
      keyValue("operation", "receiveRequest"),
      keyValue("headers", headers)
    );

    tagExtractor.extractTo(request, span::setAttribute);

    context.putLocal(ACTIVE_CONTEXT, tracingContext.with(span));
    context.putLocal(ACTIVE_SPAN, span);

    return span;
  }

  @Override
  public <R> void sendResponse(
    final Context context,
    final R response,
    final Span span,
    final Throwable failure,
    final TagExtractor<R> tagExtractor) {

    if (span == null) {
      return;
    }

    logger.debug("{} {}",
      keyValue("span", span.getClass()),
      keyValue("operation", "sendResponse"),
      failure
    );

    if (failure != null) {
      span.recordException(failure);
    }

    if (response != null) {
      tagExtractor.extractTo(response, span::setAttribute);
    }

    span.end();
  }

  @Override
  public <R> Span sendRequest(
    final Context context,
    final SpanKind kind,
    final TracingPolicy policy,
    final R request,
    final String operation,
    final BiConsumer<String, String> headers,
    final TagExtractor<R> tagExtractor) {

    logger.debug("{} {} {}",
      keyValue("operation", "sendRequest"),
      keyValue("policy", policy),
      keyValue("request", request)
    );

    if (TracingPolicy.IGNORE.equals(policy) || request == null) {
      return null;
    }

    final var spanKind = SpanKind.RPC.equals(kind) ? Kind.CLIENT : Kind.PRODUCER;

    final io.opentelemetry.context.Context tracingContext = context.getLocal(ACTIVE_CONTEXT);
    if (tracingContext == null) {

      logger.debug("No active span or context {} {}",
        keyValue("request", request),
        keyValue("operation", "sendRequest")
      );

      if (TracingPolicy.ALWAYS.equals(policy)) {

        final var span = tracer.spanBuilder(operation)
          .setSpanKind(spanKind)
          .setAttribute(SemanticAttributes.SERVICE_NAME, Tracing.SERVICE_NAME)
          .setAttribute(SemanticAttributes.SERVICE_NAMESPACE, Tracing.SERVICE_NAMESPACE)
          .startSpan();

        tagExtractor.extractTo(request, span::setAttribute);

        HttpTraceContext.getInstance().inject(current(), headers, setter);

        return span;
      }

      return null;
    }

    final var span = tracer.spanBuilder(operation)
      .setParent(tracingContext)
      .setSpanKind(spanKind)
      .setAttribute(SemanticAttributes.SERVICE_NAME, Tracing.SERVICE_NAME)
      .setAttribute(SemanticAttributes.SERVICE_NAMESPACE, Tracing.SERVICE_NAMESPACE)
      .startSpan();

    tagExtractor.extractTo(request, span::setAttribute);

    HttpTraceContext.getInstance().inject(tracingContext.with(span), headers, setter);

    logger.debug("{} {}",
      keyValue("span", span.getClass()),
      keyValue("operation", "sendRequest")
    );

    return span;
  }

  @Override
  public <R> void receiveResponse(
    final Context context,
    final R response,
    final Span span,
    final Throwable failure,
    final TagExtractor<R> tagExtractor) {

    logger.debug("{} {}", keyValue("operation", "receiveResponse"), keyValue("span", span));

    if (span == null) {
      return;
    }

    logger.debug("{} {}",
      keyValue("span", span.getClass()),
      keyValue("operation", "receiveResponse")
    );

    if (failure != null) {
      span.recordException(failure);
    }

    if (response != null) {
      tagExtractor.extractTo(response, span::setAttribute);
    }

    span.end();
  }
}
