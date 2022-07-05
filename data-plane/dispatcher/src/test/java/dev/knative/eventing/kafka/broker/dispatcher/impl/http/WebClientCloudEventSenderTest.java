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
package dev.knative.eventing.kafka.broker.dispatcher.impl.http;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleFactoryImpl;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static dev.knative.eventing.kafka.broker.dispatcher.impl.http.WebClientCloudEventSender.computeRetryPolicy;
import static dev.knative.eventing.kafka.broker.dispatcher.impl.http.WebClientCloudEventSender.isRetryableStatusCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(VertxExtension.class)
public class WebClientCloudEventSenderTest {

  @Test
  public void shouldWebClient(final Vertx vertx, final VertxTestContext context) {
    final var webClient = mock(WebClient.class);
    doNothing().when(webClient).close();

    final var consumerRecordSender = new WebClientCloudEventSender(
      vertx, webClient, "http://localhost:12345",
      DataPlaneContract.EgressConfig.getDefaultInstance());

    consumerRecordSender.close()
      .onFailure(context::failNow)
      .onSuccess(r -> context.verify(() -> {
        verify(webClient, times(1)).close();
        context.completeNow();
      }));
  }

  @Test
  @Timeout(value = 20000)
  public void shouldRetry(final Vertx vertx, final VertxTestContext context) throws ExecutionException, InterruptedException {

    final var port = 12345;
    final var retry = 5;
    final var event = CloudEventBuilder.v1()
      .withId(UUID.randomUUID().toString())
      .withSource(URI.create("/api/v1/orders"))
      .withType("dev.knative.eventing.created")
      .build();

    final var counter = new LongAdder();

    vertx.createHttpServer()
      .requestHandler(r -> {
        if (r.getHeader("Ce-Id").equalsIgnoreCase(event.getId())) {
          counter.increment();
        }
        if (counter.intValue() == 5) {
          r.response().setStatusCode(200).end();
        } else {
          r.response().setStatusCode(500).end();
        }
      })
      .listen(port, "localhost")
      .toCompletionStage()
      .toCompletableFuture()
      .get();

    final var sender = new WebClientCloudEventSender(
      vertx,
      WebClient.create(vertx),
      "http://localhost:" + port,
      DataPlaneContract.EgressConfig.newBuilder()
        .setBackoffDelay(100L)
        .setTimeout(1000L)
        .setBackoffPolicy(DataPlaneContract.BackoffPolicy.Linear)
        .setRetry(retry)
        .build()
    );

    final var success = new AtomicBoolean(false);

    sender.send(event)
      .onFailure(context::failNow)
      .onSuccess(v -> success.set(true));

    await().untilTrue(success);
    await().untilAdder(counter, is(equalTo(5L)));

    // Verify that after some time counter is still equal to 5.
    Thread.sleep(10000L);
    await().untilAdder(counter, is(equalTo(5L)));

    sender.close().onSuccess(v -> context.completeNow());
  }

  @Test
  @Timeout(value = 20000)
  public void shouldRetryAndFail(final Vertx vertx, final VertxTestContext context) throws ExecutionException, InterruptedException {

    final var port = 12345;
    final var retry = 5;
    final var event = CloudEventBuilder.v1()
      .withId(UUID.randomUUID().toString())
      .withSource(URI.create("/api/v1/orders"))
      .withType("dev.knative.eventing.created")
      .build();

    final var counter = new LongAdder();

    vertx.createHttpServer()
      .requestHandler(r -> {
        if (r.getHeader("Ce-Id").equalsIgnoreCase(event.getId())) {
          counter.increment();
        }
        if (counter.intValue() > retry+1) {
          r.response().setStatusCode(200).end();
        } else {
          r.response().setStatusCode(500).end();
        }
      })
      .listen(port, "localhost")
      .toCompletionStage()
      .toCompletableFuture()
      .get();

    final var sender = new WebClientCloudEventSender(
      vertx,
      WebClient.create(vertx),
      "http://localhost:" + port,
      DataPlaneContract.EgressConfig.newBuilder()
        .setBackoffDelay(100L)
        .setTimeout(100L)
        .setBackoffPolicy(DataPlaneContract.BackoffPolicy.Linear)
        .setRetry(retry)
        .build()
    );

    final var success = new AtomicBoolean(true);

    sender.send(event)
      .onFailure(v -> success.set(false))
      .onSuccess(v -> success.set(true));

    await().untilFalse(success);
    await().untilAdder(counter, is(equalTo(6L)));

    // Verify that after some time counter is still equal to 6.
    Thread.sleep(10000L);
    await().untilAdder(counter, is(equalTo(6L)));

    sender.close().onSuccess(v -> context.completeNow());
  }

  @Test
  @Timeout(value = 20000)
  public void shouldTimeoutAndFail(final Vertx vertx, final VertxTestContext context) throws ExecutionException, InterruptedException {

    final var port = 12345;
    final var retry = 5;
    final var timeout = 200L;
    final var event = CloudEventBuilder.v1()
      .withId(UUID.randomUUID().toString())
      .withSource(URI.create("/api/v1/orders"))
      .withType("dev.knative.eventing.created")
      .build();

    final var counter = new LongAdder();

    vertx.createHttpServer()
      .requestHandler(r -> {
        if (r.getHeader("Ce-Id").equalsIgnoreCase(event.getId())) {
          counter.increment();
        }
        if (counter.intValue() > retry + 1) {
          r.response().setStatusCode(200).end();
        } else {
          vertx.setTimer(timeout + 500, v -> r.response().setStatusCode(200).end());
        }
      })
      .listen(port, "localhost")
      .toCompletionStage()
      .toCompletableFuture()
      .get();

    final var sender = new WebClientCloudEventSender(
      vertx,
      WebClient.create(vertx),
      "http://localhost:" + port,
      DataPlaneContract.EgressConfig.newBuilder()
        .setBackoffDelay(100L)
        .setTimeout(100L)
        .setBackoffPolicy(DataPlaneContract.BackoffPolicy.Linear)
        .setRetry(retry)
        .build()
    );

    final var success = new AtomicBoolean(true);

    sender.send(event)
      .onFailure(v -> success.set(false))
      .onSuccess(v -> success.set(true));

    await().untilFalse(success);
    await().untilAdder(counter, is(equalTo(retry+1L)));

    // Verify that after some time counter is still equal to 6.
    Thread.sleep(10000L);
    await().untilAdder(counter, is(equalTo(retry+1L)));

    sender.close().onSuccess(v -> context.completeNow());
  }

  @ParameterizedTest
  @MethodSource("retryableStatusCodes")
  public void shouldRetryRetryableStatusCodes(final Integer statusCode) {
    assertThat(isRetryableStatusCode(statusCode)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("nonRetryableStatusCodes")
  public void shouldNotRetryNonRetryableStatusCodes(final Integer statusCode) {
    assertThat(isRetryableStatusCode(statusCode)).isFalse();
  }

  public static Stream<Integer> nonRetryableStatusCodes() {
    return Stream.concat(
      Stream.concat(
        IntStream.range(200, 404).boxed(),
        IntStream.range(405, 408).boxed()
      ),
      Stream.concat(
        IntStream.range(410, 429).boxed(),
        IntStream.range(430, 500).boxed()
      )
    );
  }

  public static Stream<Integer> retryableStatusCodes() {
    return Stream.concat(
      IntStream.range(500, 600).boxed(),
      IntStream.of(404, 408, 409, 429).boxed()
    );
  }

  @Test
  public void linearBackoffPolicy() {

    final var policy = computeRetryPolicy(DataPlaneContract.EgressConfig.newBuilder()
      .setRetry(10)
      .setBackoffPolicy(DataPlaneContract.BackoffPolicy.Linear)
      .setBackoffDelay(100)
      .build());

    final var delay = policy.apply(5);

    assertThat(delay).isEqualTo(100 * 5);
  }

  @Test
  public void exponentialBackoffPolicy() {

    final var policy = computeRetryPolicy(DataPlaneContract.EgressConfig.newBuilder()
      .setRetry(10)
      .setBackoffPolicy(DataPlaneContract.BackoffPolicy.Exponential)
      .setBackoffDelay(100)
      .build());

    final var delay = policy.apply(5);

    assertThat(delay).isEqualTo((long) (100 * Math.pow(2, 5)));
  }

  @Test
  public void exponentialBackoffPolicyByDefault() {

    final var policy = computeRetryPolicy(DataPlaneContract.EgressConfig.newBuilder()
      .setRetry(10)
      .setBackoffPolicy(DataPlaneContract.BackoffPolicy.Exponential)
      .setBackoffDelay(100)
      .build());

    final var delay = policy.apply(5);

    assertThat(delay).isEqualTo((long) (100 * Math.pow(2, 5)));
  }

  @Test
  public void noRetry() {

    final var policy = computeRetryPolicy(null);

    final var delay = policy.apply(Double.valueOf(Math.random()).intValue());

    assertThat(delay).isEqualTo(0);
  }

}
