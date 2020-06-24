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

package dev.knative.eventing.kafka.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ConsumerVerticleTest {

  @Test
  public void subscribedToTopic(final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final var topic = "topic1";

    final var kafkaConsumer = KafkaConsumer.create(vertx, consumer);
    final var verticle = new ConsumerVerticle<>(kafkaConsumer, topic, record -> fail());

    final Promise<Void> promise = Promise.promise();
    verticle.start(promise);

    promise.future()
        .onSuccess(ignored -> {
          assertThat(consumer.subscription()).containsExactlyInAnyOrder(topic);
          assertThat(consumer.closed()).isFalse();
          context.completeNow();
        })
        .onFailure(Assertions::fail);
  }

  @Test
  public void stop(final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final var topic = "topic1";

    final var kafkaConsumer = KafkaConsumer.create(vertx, consumer);
    final var verticle = new ConsumerVerticle<>(kafkaConsumer, topic, record -> fail());

    final Promise<Void> promise = Promise.promise();
    verticle.start((Promise<Void>) null);
    verticle.stop(promise);

    promise.future()
        .onSuccess(ignored -> {
          assertThat(consumer.closed()).isTrue();
          context.completeNow();
        })
        .onFailure(Assertions::fail);

  }
}