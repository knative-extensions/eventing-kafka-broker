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
package dev.knative.eventing.kafka.broker.core.utils;

import io.vertx.core.http.HttpServerOptions;
import java.util.Objects;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConfigurationsTest {

  private static final String FILE_NAME = "configurations.properties";
  private static final String IDLE_TIMEOUT_KEY = "idleTimeout";
  private static final String HANDLE_100_CONTINUE_AUTOMATICALLY_KEY =
      "handle100ContinueAutomatically";

  @Test
  public void shouldGetPropertiesFromFilePath() {

    final var config =
        Configurations.getProperties(
            Objects.requireNonNull(getClass().getClassLoader().getResource(FILE_NAME)).getFile());

    final var idleTimeout = config.get(IDLE_TIMEOUT_KEY);
    final var handle100ContinueAutomatically = config.get(HANDLE_100_CONTINUE_AUTOMATICALLY_KEY);

    Assertions.assertThat(idleTimeout).isEqualTo("42");
    Assertions.assertThat(handle100ContinueAutomatically).isEqualTo("true");
  }

  @Test
  public void shouldGetPropertiesFromFilePathAsJsonObject() {

    final var config =
        Configurations.getPropertiesAsJson(
            Objects.requireNonNull(getClass().getClassLoader().getResource(FILE_NAME)).getFile());

    final var idleTimeout = config.getInteger(IDLE_TIMEOUT_KEY);
    final var handle100ContinueAutomatically =
        config.getBoolean(HANDLE_100_CONTINUE_AUTOMATICALLY_KEY);

    Assertions.assertThat(idleTimeout).isEqualTo(42);
    Assertions.assertThat(handle100ContinueAutomatically).isTrue();
  }

  @Test
  public void shouldSetHttpServerOptions() {

    final var config =
        Configurations.getPropertiesAsJson(
            Objects.requireNonNull(getClass().getClassLoader().getResource(FILE_NAME)).getFile());

    final var httpServerOptions = new HttpServerOptions(config);

    Assertions.assertThat(httpServerOptions.getIdleTimeout()).isEqualTo(42);
    Assertions.assertThat(httpServerOptions.isHandle100ContinueAutomatically()).isTrue();
  }
}
