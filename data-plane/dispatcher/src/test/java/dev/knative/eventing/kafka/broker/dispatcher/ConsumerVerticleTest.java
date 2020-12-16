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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ConsumerVerticleTest {

  static {
    BackendRegistries.setupBackend(
        new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void subscribedToTopic(final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final var topic = "topic1";

    final var verticle =
        new ConsumerVerticle<>(
            v -> KafkaConsumer.create(v, consumer),
            Set.of(topic),
            (a, b) ->
                new ConsumerRecordHandler<>(
                    ConsumerRecordSender.create(
                        Future.failedFuture("subscriber send called"), Future.succeededFuture()),
                    value -> false,
                    (ConsumerRecordOffsetStrategy<Object, Object>)
                        mock(ConsumerRecordOffsetStrategy.class),
                    new SinkResponseHandlerMock<>(
                        Future::succeededFuture, response -> Future.succeededFuture()),
                    ConsumerRecordSender.create(
                        Future.failedFuture("DLQ send called"), Future.succeededFuture())));

    final Promise<String> promise = Promise.promise();
    vertx.deployVerticle(verticle, promise);

    promise
        .future()
        .onSuccess(
            ignored ->
                context.verify(
                    () -> {
                      assertThat(consumer.subscription()).containsExactlyInAnyOrder(topic);
                      assertThat(consumer.closed()).isFalse();
                      context.completeNow();
                    }))
        .onFailure(Assertions::fail);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void stop(final Vertx vertx, final VertxTestContext context) {
    final var consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final var topic = "topic1";

    final var verticle =
        new ConsumerVerticle<>(
            v -> KafkaConsumer.create(v, consumer),
            Set.of(topic),
            (a, b) ->
                new ConsumerRecordHandler<>(
                    ConsumerRecordSender.create(
                        Future.failedFuture("subscriber send called"), Future.succeededFuture()),
                    value -> false,
                    (ConsumerRecordOffsetStrategy<Object, Object>)
                        mock(ConsumerRecordOffsetStrategy.class),
                    new SinkResponseHandlerMock<>(
                        Future::succeededFuture, response -> Future.succeededFuture()),
                    ConsumerRecordSender.create(
                        Future.failedFuture("DLQ send called"), Future.succeededFuture())));

    final Promise<String> deployPromise = Promise.promise();
    vertx.deployVerticle(verticle, deployPromise);

    final Promise<Void> undeployPromise = Promise.promise();

    deployPromise
        .future()
        .onSuccess(deploymentID -> vertx.undeploy(deploymentID, undeployPromise))
        .onFailure(context::failNow);

    undeployPromise
        .future()
        .onSuccess(
            ignored -> {
              assertThat(consumer.closed()).isTrue();
              context.completeNow();
            })
        .onFailure(context::failNow);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCloseEverything(final Vertx vertx, final VertxTestContext context) {
    final var topics = new String[] {"a"};
    final KafkaConsumer<Object, Object> consumer = mock(KafkaConsumer.class);

    when(consumer.close()).thenReturn(Future.succeededFuture());
    when(consumer.subscribe((Set<String>) any(), any()))
        .then(
            answer -> {
              final Handler<AsyncResult<Void>> callback = answer.getArgument(1);
              callback.handle(Future.succeededFuture());
              return consumer;
            });

    final var mockConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    when(consumer.unwrap()).thenReturn(mockConsumer);

    mockConsumer.schedulePollTask(
        () -> {
          mockConsumer.unsubscribe();

          mockConsumer.assign(
              Arrays.stream(topics).map(t -> new TopicPartition(t, 0)).collect(Collectors.toSet()));
        });

    final var consumerRecordSenderClosed = new AtomicBoolean(false);
    final var dlsSenderClosed = new AtomicBoolean(false);
    final var sinkClosed = new AtomicBoolean(false);

    final var verticle =
        new ConsumerVerticle<>(
            v -> consumer,
            Arrays.stream(topics).collect(Collectors.toSet()),
            (v, c) ->
                new ConsumerRecordHandler<>(
                    new ConsumerRecordSenderMock<>(
                        () -> {
                          consumerRecordSenderClosed.set(true);
                          return Future.succeededFuture();
                        },
                        record -> Future.succeededFuture()),
                    ce -> true,
                    (ConsumerRecordOffsetStrategy<Object, Object>)
                        mock(ConsumerRecordOffsetStrategy.class),
                    new SinkResponseHandlerMock<>(
                        () -> {
                          sinkClosed.set(true);
                          return Future.succeededFuture();
                        },
                        response -> Future.succeededFuture()),
                    new ConsumerRecordSenderMock<>(
                        () -> {
                          dlsSenderClosed.set(true);
                          return Future.succeededFuture();
                        },
                        record -> Future.succeededFuture())));

    vertx
        .deployVerticle(verticle)
        .onFailure(context::failNow)
        .onSuccess(
            r ->
                vertx
                    .undeploy(r)
                    .onFailure(context::failNow)
                    .onSuccess(
                        ignored ->
                            context.verify(
                                () -> {
                                  assertThat(consumerRecordSenderClosed.get()).isTrue();
                                  assertThat(sinkClosed.get()).isTrue();
                                  assertThat(dlsSenderClosed.get()).isTrue();
                                  verify(consumer, times(1)).close();

                                  context.completeNow();
                                })));
  }
}
