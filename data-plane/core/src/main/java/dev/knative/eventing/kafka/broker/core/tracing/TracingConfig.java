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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public final class TracingConfig {

  private static final Logger logger = LoggerFactory.getLogger(TracingConfig.class);

  private static final String DEFAULT_SERVICE_NAME = "Knative";
  private static final String SERVICE_NAME = fromEnvOrDefault("SERVICE_NAME", TracingConfig.DEFAULT_SERVICE_NAME);
  private static final String SERVICE_NAMESPACE =
    fromEnvOrDefault("SERVICE_NAMESPACE", TracingConfig.DEFAULT_SERVICE_NAME);
  private static final AttributeKey<String> SERVICE_NAME_KEY = AttributeKey.stringKey("service.name");
  private static final AttributeKey<String> SERVICE_NAMESPACE_KEY = AttributeKey.stringKey("service.namespace");

  public final static String TRACE_ID_KEY = "traceId";

  private final Backend backend;
  private final String url;
  private final float samplingRate;
  private final HeadersFormat headersFormat;

  TracingConfig(final Backend backend, final String url, final float samplingRate, final HeadersFormat headersFormat) {
    if (!backend.equals(Backend.UNKNOWN) && !URI.create(url).isAbsolute()) {
      throw new IllegalArgumentException(String.format(
        "Backend is %s but the endpoint isn't an absolute URI: %s",
        backend,
        url
      ));
    }

    this.backend = backend;
    this.url = url;
    this.headersFormat = headersFormat;
    if (backend.equals(Backend.UNKNOWN)) {
      this.samplingRate = 0F;
    } else {
      this.samplingRate = Math.min(1, Math.max(samplingRate, 0));
    }
  }

  public OpenTelemetrySdk setup() {
    logger.info(
      "Registering tracing configurations {} {} {} {}",
      keyValue("backend", getBackend()),
      keyValue("sampleRate", getSamplingRate()),
      keyValue("loggingDebugEnabled", logger.isDebugEnabled()),
      keyValue("headersFormat", getHeadersFormat())
    );

    SdkTracerProviderBuilder tracerProviderBuilder = SdkTracerProvider.builder();

    tracerProviderBuilder.setResource(
      Resource.create(Attributes.of(
        SERVICE_NAME_KEY, SERVICE_NAME,
        SERVICE_NAMESPACE_KEY, SERVICE_NAMESPACE
      ))
    );
    tracerProviderBuilder.setSampler(
      Sampler.parentBased(Sampler.traceIdRatioBased(getSamplingRate()))
    );

    if (logger.isDebugEnabled()) {
      logger.debug("Add logging processor");
      tracerProviderBuilder.addSpanProcessor(
        SimpleSpanProcessor.create(new LoggingSpanExporter())
      );
    }
    if (getBackend().equals(Backend.ZIPKIN)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Add Zipkin simple processor");
        tracerProviderBuilder.addSpanProcessor(
          SimpleSpanProcessor.create(zipkinExporter(this))
        );
      } else {
        logger.debug("Add Zipkin batch processor");
        tracerProviderBuilder.addSpanProcessor(
          BatchSpanProcessor
            .builder(zipkinExporter(this))
            .build()
        );
      }
    }

    OpenTelemetrySdkBuilder sdkBuilder = OpenTelemetrySdk.builder();
    sdkBuilder.setTracerProvider(
      tracerProviderBuilder.build()
    );

    final var contextPropagators = switch (getHeadersFormat()) {
      case B3_MULTI_HEADER -> ContextPropagators.create(B3Propagator.injectingMultiHeaders());
      case B3_SINGLE_HEADER -> ContextPropagators.create(B3Propagator.injectingSingleHeader());
      default -> ContextPropagators.create(W3CTraceContextPropagator.getInstance());
    };
    sdkBuilder.setPropagators(contextPropagators);

    return sdkBuilder.buildAndRegisterGlobal();
  }

  Backend getBackend() {
    return backend;
  }

  String getUrl() {
    return url;
  }

  float getSamplingRate() {
    return samplingRate;
  }

  HeadersFormat getHeadersFormat() {
    return headersFormat;
  }

  @Override
  public String toString() {
    return "TracingConfig{" +
      "backend=" + backend +
      ", url='" + url + '\'' +
      ", samplingRate=" + samplingRate +
      ", headersFormat=" + headersFormat +
      '}';
  }

  // Helper methods

  private static Path backendPath(final String root) {
    return pathOf(root, "backend");
  }

  private static Path sampleRatePath(final String root) {
    return pathOf(root, "sample-rate");
  }

  private static Path headersFormatPath(final String root) {
    return pathOf(root, "headers-format");
  }

  private static SpanExporter zipkinExporter(TracingConfig tracingConfig) {
    return ZipkinSpanExporter
      .builder()
      .setEndpoint(tracingConfig.getUrl())
      .build();
  }

  private static String fromEnvOrDefault(final String key, final String defaultValue) {
    final var v = System.getenv(key);

    if (v == null || v.isBlank()) {
      return defaultValue;
    }

    return v;
  }

  private static Path pathOf(final String root, final String key) {
    if (root.endsWith("/")) {
      return Path.of(root + key);
    }
    return Path.of(root + "/" + key);
  }

  // Parser and builder

  static class Parser {

    static Backend backend(final InputStream in) throws IOException {
      return Backend.from(trim(in));
    }

    static String URL(final InputStream in) throws IOException {
      return trim(in);
    }

    static Float SamplingRate(final InputStream in) throws IOException {
      final var s = trim(in);
      if (s.isBlank()) {
        return 0F;
      }

      return Float.valueOf(s);
    }

    static HeadersFormat HeadersFormat(InputStream in) throws IOException {
      return HeadersFormat.from(trim(in));
    }

    private static String trim(InputStream in) throws IOException {
      return new String(in.readAllBytes()).trim();
    }
  }

  public static TracingConfig fromDir(final String path) throws IOException {
    final var backendPath = backendPath(path);
    if (!Files.exists(backendPath)) {
      return new TracingConfig(Backend.UNKNOWN, null, 0, HeadersFormat.W3C);
    }

    var sampleRate = 0F;
    var backend = Backend.UNKNOWN;
    var endpoint = "";
    var headsFormat = HeadersFormat.W3C;

    try (final var backendFile = new FileInputStream(backendPath.toString())) {
      backend = Parser.backend(backendFile);
    }

    if (backend.equals(Backend.UNKNOWN)) {
      return new TracingConfig(Backend.UNKNOWN, null, 0, HeadersFormat.W3C);
    }

    final var sampleRatePath = sampleRatePath(path);
    if (Files.exists(sampleRatePath)) {
      try (final var samplingRate = new FileInputStream(sampleRatePath.toString())) {
        sampleRate = Parser.SamplingRate(samplingRate);
      }
    }

    if (backend.equals(Backend.ZIPKIN)) {
      final var zipkinPath = pathOf(path, "zipkin-endpoint");
      final var headsFormatPath = headersFormatPath(path);
      if (Files.exists(zipkinPath)) {
        try (final var url = new FileInputStream(zipkinPath.toString())) {
          endpoint = Parser.URL(url);
        }
      }
      if (Files.exists(headsFormatPath)) {
        try (final var headsFormatFile = new FileInputStream(headsFormatPath.toString())) {
          final var parsed = Parser.HeadersFormat(headsFormatFile);
          //B3 propagation is available when backend is zipkin only
          if (parsed.equals(HeadersFormat.B3_MULTI_HEADER)
            || parsed.equals(HeadersFormat.B3_SINGLE_HEADER)) {
            headsFormat = parsed;
          }
        }
      }
    }

    return new TracingConfig(backend, endpoint, sampleRate, headsFormat);
  }

  // Backend definition

  enum Backend {
    ZIPKIN,
    UNKNOWN;

    public static Backend from(final String s) {
      return switch (s.trim().toLowerCase()) {
        case "zipkin" -> ZIPKIN;
        default -> UNKNOWN;
      };
    }
  }

  enum HeadersFormat {
    B3_MULTI_HEADER,
    B3_SINGLE_HEADER,
    W3C;

    public static HeadersFormat from(final String s) {
      return switch (s.trim().toLowerCase()) {
        case "b3-multi-header" -> B3_MULTI_HEADER;
        case "b3-single-header" -> B3_SINGLE_HEADER;
        default -> W3C;
      };
    }
  }

}
