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
package dev.knative.eventing.kafka.broker.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.utils.BaseEnv;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class MetricsTest {

  static {
    BackendRegistries.setupBackend(
        new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME));
  }

  @Test
  public void get() {
    final var metricsOptions = Metrics.getOptions(new BaseEnv(s -> "1"));
    assertThat(metricsOptions.isEnabled()).isTrue();
  }

  @Test
  public void shouldRegisterAndCloseProducerMetricsThread(
      final Vertx vertx, final VertxTestContext context) {
    final var producer = new MockProducer<>();

    final var meterBinder = Metrics.register(producer);

    Metrics.close(vertx, meterBinder)
        .onFailure(context::failNow)
        .onSuccess(r -> context.completeNow());
  }

  @Test
  public void shouldRegisterAndCloseConsumerMetricsThread(
      final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    final var meterBinder = Metrics.register(consumer);

    Metrics.close(vertx, meterBinder)
        .onFailure(context::failNow)
        .onSuccess(r -> context.completeNow());
  }
}
