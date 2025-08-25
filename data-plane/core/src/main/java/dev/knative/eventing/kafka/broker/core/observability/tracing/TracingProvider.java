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
package dev.knative.eventing.kafka.broker.core.observability.tracing;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

import dev.knative.eventing.kafka.broker.core.observability.ObservabilityConfig;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.logging.otlp.internal.traces.OtlpStdoutSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TracingProvider {

    private static final Logger logger = LoggerFactory.getLogger(TracingProvider.class);

    private static final String DEFAULT_SERVICE_NAME = "Knative";
    private static final String SERVICE_NAME = fromEnvOrDefault("SERVICE_NAME", TracingProvider.DEFAULT_SERVICE_NAME);
    private static final String SERVICE_NAMESPACE =
            fromEnvOrDefault("SERVICE_NAMESPACE", TracingProvider.DEFAULT_SERVICE_NAME);
    private static final AttributeKey<String> SERVICE_NAME_KEY = AttributeKey.stringKey("service.name");
    private static final AttributeKey<String> SERVICE_NAMESPACE_KEY = AttributeKey.stringKey("service.namespace");

    public static final String TRACE_ID_KEY = "traceId";

    private final ObservabilityConfig.TracingConfig tracingConfig;

    public TracingProvider(ObservabilityConfig.TracingConfig tracingConfig) {
        this.tracingConfig = tracingConfig;
    }

    public OpenTelemetrySdk setup() {
        logger.info(
                "Registering tracing configurations {} {} {}",
                keyValue("protocol", tracingConfig.protocol()),
                keyValue("sampleRate", tracingConfig.samplingRate()),
                keyValue("loggingDebugEnabled", logger.isDebugEnabled()));

        SdkTracerProviderBuilder tracerProviderBuilder = SdkTracerProvider.builder();

        tracerProviderBuilder.setResource(Resource.create(Attributes.of(
                SERVICE_NAME_KEY, SERVICE_NAME,
                SERVICE_NAMESPACE_KEY, SERVICE_NAMESPACE)));
        tracerProviderBuilder.setSampler(Sampler.parentBased(Sampler.traceIdRatioBased(tracingConfig.samplingRate())));

        if (logger.isDebugEnabled()) {
            logger.debug("Add logging processor");
            tracerProviderBuilder.addSpanProcessor(
                    SimpleSpanProcessor.create(OtlpStdoutSpanExporter.builder().build()));
        }

        tracerProviderBuilder.addSpanProcessor(SimpleSpanProcessor.create(otlpExporter(tracingConfig)));

        OpenTelemetrySdkBuilder sdkBuilder = OpenTelemetrySdk.builder();
        sdkBuilder.setTracerProvider(tracerProviderBuilder.build());

        final var contextPropagators = ContextPropagators.create(TextMapPropagator.composite(
                W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance()));
        sdkBuilder.setPropagators(contextPropagators);

        return sdkBuilder.buildAndRegisterGlobal();
    }

    private static SpanExporter otlpExporter(final ObservabilityConfig.TracingConfig tracingConfig) {
        return switch (tracingConfig.protocol()) {
            case NONE -> SpanExporter.composite();
            case OTLP_HTTP -> OtlpHttpSpanExporter.builder()
                    .setEndpoint(tracingConfig.endpoint())
                    .build();
            case OTLP_GRPC -> OtlpGrpcSpanExporter.builder()
                    .setEndpoint(tracingConfig.endpoint())
                    .build();
            case STDOUT -> OtlpStdoutSpanExporter.builder().build();
        };
    }

    private static String fromEnvOrDefault(final String key, final String defaultValue) {
        final var v = System.getenv(key);

        if (v == null || v.isBlank()) {
            return defaultValue;
        }

        return v;
    }
}
