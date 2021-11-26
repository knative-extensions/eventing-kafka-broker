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
import dev.knative.eventing.kafka.broker.core.tracing.TracingConfig.HeadersFormat;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TracingConfigTest {

  @Test
  public void shouldParseConfigGivenDirectory() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " zipkin ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  http://localhost:9241/v2/api/spans/     ");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.ZIPKIN);
    assertThat(config.getSamplingRate()).isEqualTo(0.1F);
    assertThat(config.getUrl()).isEqualTo("http://localhost:9241/v2/api/spans/");
    assertThat(config.getHeadersFormat()).isEqualTo(HeadersFormat.W3C);
  }

  @Test
  public void shouldParseConfigB3SingleHeaderGivenDirectory() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " zipkin ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  http://localhost:9241/v2/api/spans/     ");
    write(dir, "/headers-format", " b3-single-header  ");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.ZIPKIN);
    assertThat(config.getSamplingRate()).isEqualTo(0.1F);
    assertThat(config.getUrl()).isEqualTo("http://localhost:9241/v2/api/spans/");
    assertThat(config.getHeadersFormat()).isEqualTo(HeadersFormat.B3_SINGLE_HEADER);
  }

  @Test
  public void shouldParseConfigB3MultiHeaderGivenDirectory() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " zipkin ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  http://localhost:9241/v2/api/spans/     ");
    write(dir, "/headers-format", " b3-multi-header  ");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.ZIPKIN);
    assertThat(config.getSamplingRate()).isEqualTo(0.1F);
    assertThat(config.getUrl()).isEqualTo("http://localhost:9241/v2/api/spans/");
    assertThat(config.getHeadersFormat()).isEqualTo(HeadersFormat.B3_MULTI_HEADER);
  }


  @Test
  public void shouldReturnUnknownBackendWhenBackendContainsUnknownName() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " 1234 ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  http://localhost:9241/v2/api/spans/     ");
    write(dir, "/headers-format", " b3-multi-header ");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.UNKNOWN);
  }

  @Test
  public void shouldReturnW3CHeadersFormatWhenBackendContainsUnknownName() throws IOException {
    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " 1234 ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  http://localhost:9241/v2/api/spans/     ");
    write(dir, "/headers-format", " b3-multi-header ");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getHeadersFormat()).isEqualTo(HeadersFormat.W3C);
  }

  @Test
  public void shouldReturnW3CHeadersFormatWhenNoFilesArePresent() throws IOException {
    final var dir = Files.createTempDirectory("tracing");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getHeadersFormat()).isEqualTo(HeadersFormat.W3C);
  }

  @Test
  public void shouldReturnUnknownBackendWhenNoFilesArePresent() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.UNKNOWN);
  }

  @Test
  public void shouldReturnW3CHeadersFormatWhenDirectoryIsNotPresent() throws IOException {

    final var config = TracingConfig.fromDir("/tmp/" + UUID.randomUUID().toString());

    assertThat(config.getHeadersFormat()).isEqualTo(HeadersFormat.W3C);
  }

  @Test
  public void shouldReturnUnknownBackendWhenDirectoryIsNotPresent() throws IOException {

    final var config = TracingConfig.fromDir("/tmp/" + UUID.randomUUID().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.UNKNOWN);
  }

  @Test
  public void throwWhenURLIsNotAbsolute() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " zipkin ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  /v2/api/spans/     ");

    assertThatThrownBy(() -> TracingConfig.fromDir(dir.toAbsolutePath().toString()));
  }

  @Test
  public void shouldNotThrowWhenURLIsNotAbsoluteAndBackendUnknown() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " 1234 ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  /v2/api/spans/     ");

    assertDoesNotThrow(() -> TracingConfig.fromDir(dir.toAbsolutePath().toString()));
  }

  @Test
  public void setupShouldNotFailWhenBackendIsUnknown() {
    assertThatNoException().isThrownBy(() -> new TracingConfig(Backend.UNKNOWN, null, 0F, HeadersFormat.W3C).setup());
  }

  private static void write(final Path root, final String name, final String s) throws IOException {
    try (final var f = Files.newOutputStream(Path.of(root.toAbsolutePath().toString() + name), CREATE, APPEND)) {
      f.write(s.getBytes());
    }
  }
}
