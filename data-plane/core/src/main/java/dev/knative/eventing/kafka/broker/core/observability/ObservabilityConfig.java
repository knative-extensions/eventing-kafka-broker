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

package dev.knative.eventing.kafka.broker.core.observability;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class ObservabilityConfig {
    public ObservabilityConfig(MetricsConfig metricsConfig, TracingConfig tracingConfig) {
        this.metricsConfig = metricsConfig;
        this.tracingConfig = tracingConfig;
    }

    public static ObservabilityConfig fromDir(final String path) throws IOException {
        final MetricsConfig metricsConfig = MetricsConfig.fromDir(path);
        final TracingConfig tracingConfig = TracingConfig.fromDir(path);

        return new ObservabilityConfig(metricsConfig, tracingConfig);
    }

    public record MetricsConfig(MetricsProtocol protocol, String endpoint, String exportInterval) {
        private static final Integer DEFAULT_METRICS_PORT = 9090;

        public static MetricsConfig fromDir(final String path) throws IOException {
            var exportInterval = "60s"; // 60s is the default for all knative components
            var protocol = MetricsProtocol.NONE;
            var endpoint = "";

            final var protocolPath = metricsProtocolPath(path);
            if (Files.exists(protocolPath)) {
                try (final var protocolFile = new FileInputStream(protocolPath.toFile())) {
                    protocol = Parser.metricsProtocol(protocolFile);
                }
            }

            final var exportIntervalPath = metricsExportIntervalPath(path);
            if (Files.exists(exportIntervalPath)) {
                try (final var exportIntervalFile = new FileInputStream(exportIntervalPath.toFile())) {
                    exportInterval = Parser.exportInterval(exportIntervalFile);
                }
            }

            final var endpointPath = metricsEndpointPath(path);
            if (Files.exists(endpointPath)) {
                try (final var endpointFile = new FileInputStream(endpointPath.toFile())) {
                    endpoint = Parser.endpoint(endpointFile);
                }
            }

            return new MetricsConfig(protocol, endpoint, exportInterval);
        }

        public Integer getMetricsPort() {
            try {
                URI metricsUrl = new URI(this.endpoint);

                var port = metricsUrl.getPort();
                if (port <= 0) {
                    return DEFAULT_METRICS_PORT;
                }

                return port;
            } catch (URISyntaxException e) {
                return DEFAULT_METRICS_PORT;
            }
        }

        public String getMetricsPath() {
            try {
                URI metricsUrl = new URI(this.endpoint);

                var path = metricsUrl.getPath();
                return Optional.ofNullable(path).filter(p -> !p.isEmpty()).orElse("/metrics");
            } catch (URISyntaxException e) {
                return "/metrics";
            }
        }
    }

    public record TracingConfig(TracingProtocol protocol, String endpoint, Float samplingRate) {
        public static TracingConfig fromDir(final String path) throws IOException {
            var samplingRate = 0F;
            var protocol = TracingProtocol.NONE;
            var endpoint = "";

            final var samplingRatePath = tracingSamplingRatePath(path);
            if (Files.exists(samplingRatePath)) {
                try (final var samplingRateFile = new FileInputStream(samplingRatePath.toFile())) {
                    samplingRate = Parser.samplingRate(samplingRateFile);
                }
            }

            final var protocolPath = tracingProtocolPath(path);
            if (Files.exists(protocolPath)) {
                try (final var protocolFile = new FileInputStream(protocolPath.toFile())) {
                    protocol = Parser.tracingProtocol(protocolFile);
                }
            }

            final var endpointPath = tracingEndpointPath(path);
            if (Files.exists(endpointPath)) {
                try (final var endpointFile = new FileInputStream(endpointPath.toFile())) {
                    endpoint = Parser.endpoint(endpointFile);
                }
            }

            return new TracingConfig(protocol, endpoint, samplingRate);
        }
    }

    private final MetricsConfig metricsConfig;
    private final TracingConfig tracingConfig;

    public MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    public TracingConfig getTracingConfig() {
        return tracingConfig;
    }

    private static Path pathOf(final String root, final String key) {
        if (root.endsWith("/")) {
            return Path.of(root + key);
        }

        return Path.of(root + "/" + key);
    }

    private static Path metricsProtocolPath(final String root) {
        return pathOf(root, "metrics-protocol");
    }

    private static Path metricsEndpointPath(final String root) {
        return pathOf(root, "metrics-endpoint");
    }

    private static Path metricsExportIntervalPath(final String root) {
        return pathOf(root, "metrics-export-interval");
    }

    private static Path tracingProtocolPath(final String root) {
        return pathOf(root, "tracing-protocol");
    }

    private static Path tracingEndpointPath(final String root) {
        return pathOf(root, "tracing-endpoint");
    }

    private static Path tracingSamplingRatePath(final String root) {
        return pathOf(root, "tracing-sampling-rate");
    }

    static class Parser {
        static MetricsProtocol metricsProtocol(final InputStream in) throws IOException {
            return MetricsProtocol.from(trim(in));
        }

        static TracingProtocol tracingProtocol(final InputStream in) throws IOException {
            return TracingProtocol.from(trim(in));
        }

        static String endpoint(final InputStream in) throws IOException {
            return trim(in);
        }

        static Float samplingRate(final InputStream in) throws IOException {
            final var s = trim(in);
            if (s.isBlank()) {
                return 0F;
            }

            return Float.valueOf(s);
        }

        static String exportInterval(final InputStream in) throws IOException {
            final var s = trim(in);
            if (s.isBlank()) {
                return "60s";
            }

            return s;
        }

        private static String trim(final InputStream in) throws IOException {
            return new String(in.readAllBytes()).trim();
        }
    }

    public enum MetricsProtocol {
        NONE,
        OTLP_HTTP,
        OTLP_GRPC,
        PROMETHEUS;

        public static MetricsProtocol from(final String protocol) {
            return switch (protocol.trim().toLowerCase()) {
                case "grpc" -> OTLP_GRPC;
                case "http/protobuf" -> OTLP_HTTP;
                case "prometheus" -> PROMETHEUS;
                default -> NONE;
            };
        }
    }

    public enum TracingProtocol {
        NONE,
        OTLP_HTTP,
        OTLP_GRPC,
        STDOUT;

        public static TracingProtocol from(final String protocol) {
            return switch (protocol.trim().toLowerCase()) {
                case "grpc" -> OTLP_GRPC;
                case "http/protobuf" -> OTLP_HTTP;
                case "stdout" -> STDOUT;
                default -> NONE;
            };
        }
    }
}
