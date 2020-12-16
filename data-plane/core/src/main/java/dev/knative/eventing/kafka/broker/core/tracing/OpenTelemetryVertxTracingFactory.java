/*
 * Copyright 2020 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
