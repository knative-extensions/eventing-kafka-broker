package dev.knative.eventing.kafka.broker.core.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Span.Kind;
import io.vertx.core.Context;
import io.vertx.core.spi.VertxTracerFactory;
import io.vertx.core.spi.tracing.SpanKind;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingOptions;
import io.vertx.core.tracing.TracingPolicy;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

public class OpenTelemetryVertxTracingFactory implements VertxTracerFactory {

  @Override
  public VertxTracer<?, ?> tracer(final TracingOptions options) {
    return new Tracer(
      OpenTelemetry.getGlobalTracer(OpenTelemetryVertxTracingFactory.class.getName())
    ); // TODO inject name via constructor
  }

  private static class Tracer implements VertxTracer<Span, Span> {

    public static String ACTIVE_SPAN = "opentracing.span";
    public static String ACTIVE_CONTEXT = "opentracing.context";

    private final io.opentelemetry.api.trace.Tracer tracer;

    private Tracer(final io.opentelemetry.api.trace.Tracer tracer) {
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

      if (TracingPolicy.IGNORE.equals(policy) || request == null) {
        return null;
      }

      final var tracingContext = TracingContext.from(headers);
      final var span = tracer.spanBuilder(operation)
        .setParent(tracingContext)
        .setSpanKind(SpanKind.RPC.equals(kind) ? Kind.SERVER : Kind.CONSUMER)
        .startSpan();

      tagExtractor.extractTo(request, span::setAttribute);

      context.putLocal(ACTIVE_SPAN, span);
      context.putLocal(ACTIVE_CONTEXT, tracingContext);

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

      context.removeLocal(ACTIVE_SPAN);
      context.removeLocal(ACTIVE_CONTEXT);

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

      if (TracingPolicy.IGNORE.equals(policy) || request == null) {
        return null;
      }

      final Span activeSpan = context.getLocal(ACTIVE_SPAN);
      if (activeSpan == null) {
        return null;
      }
      io.opentelemetry.context.Context tracingContext = context.getLocal(ACTIVE_CONTEXT);
      if (tracingContext == null) {
        return null;
      }

      final var span = tracer.spanBuilder(operation)
        .setSpanKind(SpanKind.RPC.equals(kind) ? Kind.CLIENT : Kind.PRODUCER)
        .setParent(tracingContext)
        .startSpan();

      tagExtractor.extractTo(request, span::setAttribute);

      if (headers != null) {
        TracingContext.to(tracingContext, headers);
      }
      return span;
    }

    @Override
    public <R> void receiveResponse(
      final Context context,
      final R response,
      final Span span,
      final Throwable failure,
      final TagExtractor<R> tagExtractor) {

      if (span == null) {
        return;
      }

      if (failure != null) {
        span.recordException(failure);
      }

      if (response != null) {
        tagExtractor.extractTo(response, span::setAttribute);
      }

      span.end();
    }

    @Override
    public void close() {

    }
  }
}
