package dev.knative.eventing.kafka.broker.core.tracing;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig.Backend;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.TracerSdkManagement;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tracing {

  private static final Logger logger = LoggerFactory.getLogger(Tracing.class);

  private static final TracerSdkManagement tracerManagement = OpenTelemetrySdk.getGlobalTracerManagement();

  public static void setup(final TracingConfig tracingConfig) {

    logger.info(
      "Registering tracing configurations {} {} {} {}",
      keyValue("backend", tracingConfig.getBackend()),
      keyValue("sampleRate", tracingConfig.getSamplingRate()),
      keyValue("URL", tracingConfig.getURL()),
      keyValue("loggingDebugEnabled", logger.isDebugEnabled())
    );

    tracerManagement.updateActiveTraceConfig(
      TraceConfig.getDefault()
        .toBuilder()
        .setSampler(Sampler.traceIdRatioBased(tracingConfig.getSamplingRate()))
        .build()
    );

    if (tracingConfig.getBackend().equals(Backend.ZIPKIN)) {

      logger.debug("Add Zipkin processor");

      tracerManagement.addSpanProcessor(
        SimpleSpanProcessor // TODO move to batch span processor
          .builder(zipkinExporter(tracingConfig))
          .setExportOnlySampled(true)
          .build()
      );

    } else if (logger.isDebugEnabled()) {

      logger.debug("Add Logging processor");

      tracerManagement.addSpanProcessor(
        SimpleSpanProcessor
          .builder(new LoggingSpanExporter())
          .setExportOnlySampled(true)
          .build()
      );
    }
  }

  public static void shutdown() {
    tracerManagement.shutdown();
  }

  private static SpanExporter zipkinExporter(TracingConfig tracingConfig) {
    return ZipkinSpanExporter
      .builder()
      .setEndpoint(tracingConfig.getURL())
      .setServiceName(TracingSpan.SERVICE_NAME)
      .build();
  }
}
