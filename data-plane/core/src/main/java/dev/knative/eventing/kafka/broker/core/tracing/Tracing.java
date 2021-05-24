/*
 * Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
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

import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig.Backend;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class Tracing {

  private static final AttributeKey<String> SERVICE_NAME_KEY = AttributeKey.stringKey("service.name");
  private static final AttributeKey<String> SERVICE_NAMESPACE_KEY = AttributeKey.stringKey("service.namespace");

  public final static String SERVICE_NAME;
  public final static String SERVICE_NAMESPACE;
  public final static String TRACE_ID_KEY = "traceId";
  private static final String DEFAULT_SERVICE_NAME = "Knative";

  static {
    SERVICE_NAME = fromEnvOrDefault("SERVICE_NAME", DEFAULT_SERVICE_NAME);
    SERVICE_NAMESPACE = fromEnvOrDefault("SERVICE_NAMESPACE", DEFAULT_SERVICE_NAME);
  }

  private static final Logger logger = LoggerFactory.getLogger(Tracing.class);

  public static OpenTelemetry setup(final TracingConfig tracingConfig) {
    logger.info(
      "Registering tracing configurations {} {} {} {}",
      keyValue("backend", tracingConfig.getBackend()),
      keyValue("sampleRate", tracingConfig.getSamplingRate()),
      keyValue("URL", tracingConfig.getURL()),
      keyValue("loggingDebugEnabled", logger.isDebugEnabled())
    );

    SdkTracerProviderBuilder tracerProviderBuilder = SdkTracerProvider.builder();

    tracerProviderBuilder.setResource(
      Resource.create(Attributes.of(
        SERVICE_NAME_KEY, SERVICE_NAME,
        SERVICE_NAMESPACE_KEY, SERVICE_NAMESPACE
      ))
    );
    tracerProviderBuilder.setSampler(
      Sampler.traceIdRatioBased(tracingConfig.getSamplingRate())
    );

    if (tracingConfig.getBackend().equals(Backend.ZIPKIN)) {
      logger.debug("Add Zipkin processor");
      tracerProviderBuilder.addSpanProcessor(
        BatchSpanProcessor
          .builder(zipkinExporter(tracingConfig))
          .build()
      );

    } else if (logger.isDebugEnabled()) {
      logger.debug("Add Logging processor");
      tracerProviderBuilder.addSpanProcessor(
        SimpleSpanProcessor.create(new LoggingSpanExporter())
      );
    }

    OpenTelemetrySdkBuilder sdkBuilder = OpenTelemetrySdk.builder();
    sdkBuilder.setTracerProvider(tracerProviderBuilder.build());
    sdkBuilder.setPropagators(ContextPropagators.create(
      W3CTraceContextPropagator.getInstance()
    ));


    return sdkBuilder.build();
  }

  private static SpanExporter zipkinExporter(TracingConfig tracingConfig) {
    return ZipkinSpanExporter
      .builder()
      .setEndpoint(tracingConfig.getURL())
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
