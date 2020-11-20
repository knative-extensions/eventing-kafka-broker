package dev.knative.eventing.kafka.broker.core.tracing;

import io.opentelemetry.api.trace.Span;
import io.vertx.core.spi.VertxTracerFactory;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingOptions;

public class OpenTelemetryVertxTracingFactory implements VertxTracerFactory {

  private final io.opentelemetry.api.trace.Tracer tracer;

  public OpenTelemetryVertxTracingFactory(final io.opentelemetry.api.trace.Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public VertxTracer<Span, Span> tracer(final TracingOptions options) {
    return new OpenTelemetryTracer(this.tracer);
  }
}
