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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TracingConfigTest {

  @Test
  public void shouldParseConfigGivenDirectory() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " zipkin ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  http://localhost:9241/v2/api/spans/     ");
    write(dir, "/trace-propagation-format", "w3c, b3");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.ZIPKIN);
    assertThat(config.getSamplingRate()).isEqualTo(0.1F);
    assertThat(config.getUrl()).isEqualTo("http://localhost:9241/v2/api/spans/");
    assertThat(config.getTracePropagationFormat())
      .containsExactlyInAnyOrder(TracingConfig.TracePropagationFormat.W3C, TracingConfig.TracePropagationFormat.B3);
  }

  @Test
  public void shouldReturnUnknownBackendWhenBackendContainsUnknownName() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    write(dir, "/backend", " 1234 ");
    write(dir, "/sample-rate", " 0.1 ");
    write(dir, "/zipkin-endpoint", "  http://localhost:9241/v2/api/spans/     ");
    write(dir, "/trace-propagation-format", "w3c, b3");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.UNKNOWN);
  }

  @Test
  public void shouldReturnUnknownBackendWhenNoFilesArePresent() throws IOException {

    final var dir = Files.createTempDirectory("tracing");

    final var config = TracingConfig.fromDir(dir.toAbsolutePath().toString());

    assertThat(config.getBackend()).isEqualTo(Backend.UNKNOWN);
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
    assertThatNoException()
      .isThrownBy(() -> new TracingConfig(Backend.UNKNOWN, null, 0F, Collections.emptyList()).setup());
  }

  private static void write(final Path root, final String name, final String s) throws IOException {
    try (final var f = Files.newOutputStream(Path.of(root.toAbsolutePath().toString() + name), CREATE, APPEND)) {
      f.write(s.getBytes());
    }
  }
}
