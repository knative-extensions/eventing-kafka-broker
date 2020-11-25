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
package dev.knative.eventing.kafka.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Set;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ConsumerVerticleTest {

  static {
    BackendRegistries.setupBackend(new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME));
  }

  @Test
  public void subscribedToTopic(final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final var topic = "topic1";

    final var verticle = new ConsumerVerticle<>(
      v -> KafkaConsumer.create(v, consumer),
      Set.of(topic),
      (a, b) -> record -> fail()
    );

    final Promise<String> promise = Promise.promise();
    vertx.deployVerticle(verticle, promise);

    promise.future()
      .onSuccess(ignored -> context.verify(() -> {
        assertThat(consumer.subscription()).containsExactlyInAnyOrder(topic);
        assertThat(consumer.closed()).isFalse();
        context.completeNow();
      }))
      .onFailure(Assertions::fail);
  }

  @Test
  public void stop(final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final var topic = "topic1";

    final var verticle = new ConsumerVerticle<>(
      v -> KafkaConsumer.create(v, consumer),
      Set.of(topic),
      (a, b) -> record -> fail()
    );

    final Promise<String> deployPromise = Promise.promise();
    vertx.deployVerticle(verticle, deployPromise);

    final Promise<Void> undeployPromise = Promise.promise();

    deployPromise.future()
      .onSuccess(deploymentID -> vertx.undeploy(deploymentID, undeployPromise))
      .onFailure(context::failNow);

    undeployPromise.future()
      .onSuccess(ignored -> {
        assertThat(consumer.closed()).isTrue();
        context.completeNow();
      })
      .onFailure(context::failNow);

  }
}
