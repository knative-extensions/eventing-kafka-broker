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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class Tracing {

  public final static String SERVICE_NAME;
  public final static String SERVICE_NAMESPACE;
  public final static String TRACE_ID_KEY = "traceId";

  private static final String DEFAULT_SERVICE_NAME = "Knative";

  static {
    SERVICE_NAME = fromEnvOrDefault("SERVICE_NAME", DEFAULT_SERVICE_NAME);
    SERVICE_NAMESPACE = fromEnvOrDefault("SERVICE_NAMESPACE", DEFAULT_SERVICE_NAME);
  }

  private static final Logger logger = LoggerFactory.getLogger(Tracing.class);

  public static SdkTracerProvider setup(final TracingConfig tracingConfig) {
    MDC.put("backend", tracingConfig.getBackend().toString());
    MDC.put("sampleRate", String.valueOf(tracingConfig.getSamplingRate()));
    MDC.put("url", tracingConfig.getURL());
    logger.info(
      "Registering tracing configurations. Backend: {}, sample rate: {}, url: {}",
      tracingConfig.getBackend(),
      tracingConfig.getSamplingRate(),
      tracingConfig.getURL()
    );
    MDC.clear();

    SdkTracerProviderBuilder tracerProviderBuilder = SdkTracerProvider.builder();

    tracerProviderBuilder.setResource(
      Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, SERVICE_NAME))
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

    return tracerProviderBuilder.build();
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
