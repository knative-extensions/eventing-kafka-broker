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

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig.Backend;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.TracerSdkManagement;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tracing {

  public static final String SERVICE_NAME;
  public static final String SERVICE_NAMESPACE;
  public static final String TRACE_ID_KEY = "traceId";

  private static final String DEFAULT_SERVICE_NAME = "Knative";

  static {
    SERVICE_NAME = fromEnvOrDefault("SERVICE_NAME", DEFAULT_SERVICE_NAME);
    SERVICE_NAMESPACE = fromEnvOrDefault("SERVICE_NAMESPACE", DEFAULT_SERVICE_NAME);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Tracing.class);

  private static final TracerSdkManagement GLOBAL_TRACER_MANAGEMENT =
      OpenTelemetrySdk.getGlobalTracerManagement();

  public static void setup(final TracingConfig tracingConfig) {

    LOGGER.info(
        "Registering tracing configurations {} {} {} {}",
        keyValue("backend", tracingConfig.getBackend()),
        keyValue("sampleRate", tracingConfig.getSamplingRate()),
        keyValue("URL", tracingConfig.getUrl()),
        keyValue("loggingDebugEnabled", LOGGER.isDebugEnabled()));

    GLOBAL_TRACER_MANAGEMENT.updateActiveTraceConfig(
        TraceConfig.getDefault().toBuilder()
            .setSampler(Sampler.traceIdRatioBased(tracingConfig.getSamplingRate()))
            .build());

    if (tracingConfig.getBackend().equals(Backend.ZIPKIN)) {

      LOGGER.debug("Add Zipkin processor");

      GLOBAL_TRACER_MANAGEMENT.addSpanProcessor(
          BatchSpanProcessor.builder(zipkinExporter(tracingConfig))
              .setExportOnlySampled(true)
              .build());

    } else if (LOGGER.isDebugEnabled()) {

      LOGGER.debug("Add Logging processor");

      GLOBAL_TRACER_MANAGEMENT.addSpanProcessor(
          SimpleSpanProcessor.builder(new LoggingSpanExporter())
              .setExportOnlySampled(true)
              .build());
    }
  }

  public static void shutdown() {
    GLOBAL_TRACER_MANAGEMENT.shutdown();
  }

  private static SpanExporter zipkinExporter(final TracingConfig tracingConfig) {
    return ZipkinSpanExporter.builder()
        .setEndpoint(tracingConfig.getUrl())
        .setServiceName(SERVICE_NAME)
        .build();
  }

  private static String fromEnvOrDefault(final String key, final String defaultValue) {
    final var v = System.getenv(key);

    if (v == null || v.isBlank()) {
      return defaultValue;
    }

    return v;
  }
}
