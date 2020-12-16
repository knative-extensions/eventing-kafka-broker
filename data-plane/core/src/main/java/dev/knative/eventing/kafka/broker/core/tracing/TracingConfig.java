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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

public final class TracingConfig {

  private final Backend backend;
  private final String url;
  private final float samplingRate;

  private TracingConfig(final Backend backend, final String url, final float samplingRate) {
    if (!backend.equals(Backend.UNKNOWN) && !URI.create(url).isAbsolute()) {
      throw new IllegalArgumentException(
          String.format("Backend is %s but the endpoint isn't an absolute URI: %s", backend, url));
    }

    this.backend = backend;
    this.url = url;
    this.samplingRate = Math.min(1, Math.max(samplingRate, 0));
  }

  public static TracingConfig fromDir(final String path) throws IOException {

    final var backendPath = backendPath(path);
    if (!Files.exists(backendPath)) {
      return TracingConfig.builder().setBackend(Backend.UNKNOWN).createTracingConfig();
    }

    var sampleRate = 0F;
    var backend = Backend.UNKNOWN;
    var endpoint = "";

    try (final var backendFile = new FileInputStream(backendPath.toString())) {
      backend = Parser.backend(backendFile);
    }

    if (backend.equals(Backend.UNKNOWN)) {
      return TracingConfig.builder().createTracingConfig();
    }

    final var sampleRatePath = sampleRatePath(path);
    if (Files.exists(sampleRatePath)) {
      try (final var samplingRate = new FileInputStream(sampleRatePath.toString())) {
        sampleRate = Parser.SamplingRate(samplingRate);
      }
    }

    if (backend.equals(Backend.ZIPKIN)) {
      final var zipkinPath = pathOf(path, "zipkin-endpoint");
      if (Files.exists(zipkinPath)) {
        try (final var url = new FileInputStream(zipkinPath.toString())) {
          endpoint = Parser.URL(url);
        }
      }
    }

    return builder()
        .setBackend(backend)
        .setSamplingRate(sampleRate)
        .setUrl(endpoint)
        .createTracingConfig();
  }

  public static Builder builder() {
    return new Builder();
  }

  public enum Backend {
    ZIPKIN,
    UNKNOWN;

    public static Backend from(final String s) {
      switch (s.trim().toLowerCase()) {
        case "zipkin":
          return ZIPKIN;
        default:
          return UNKNOWN;
      }
    }
  }

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

    private static String trim(final InputStream in) throws IOException {
      return new String(in.readAllBytes()).trim();
    }
  }

  static class Builder {

    private Backend backend = Backend.UNKNOWN;
    private String url;
    private float samplingRate;

    Builder setBackend(final Backend backend) {
      this.backend = backend;
      return this;
    }

    Builder setUrl(final String url) {
      this.url = url;
      return this;
    }

    Builder setSamplingRate(final float samplingRate) {
      this.samplingRate = samplingRate;
      return this;
    }

    TracingConfig createTracingConfig() {
      if (backend.equals(Backend.UNKNOWN)) {
        samplingRate = 0F;
      }

      return new TracingConfig(backend, url, samplingRate);
    }
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

  private static Path backendPath(final String root) {
    return pathOf(root, "backend");
  }

  private static Path sampleRatePath(final String root) {
    return pathOf(root, "sample-rate");
  }

  private static Path pathOf(final String root, final String key) {
    if (root.endsWith("/")) {
      return Path.of(root + key);
    }
    return Path.of(root + "/" + key);
  }

  @Override
  public String toString() {
    return "TracingConfig{"
        + "backend="
        + backend
        + ", URL='"
        + url
        + '\''
        + ", samplingRate="
        + samplingRate
        + '}';
  }
}
