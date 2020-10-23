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

package dev.knative.eventing.kafka.broker.receiver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerImpl;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class RequestHandlerTest {

  private static final int TIMEOUT = 3;

  @Test
  public void shouldSendRecordAndTerminateRequestWithRecordProduced() throws InterruptedException {
    shouldSendRecord(false, RequestHandler.RECORD_PRODUCED);
  }

  @Test
  public void shouldSendRecordAndTerminateRequestWithFailedToProduce() throws InterruptedException {
    shouldSendRecord(true, RequestHandler.FAILED_TO_PRODUCE);
  }

  @SuppressWarnings("unchecked")
  private static void shouldSendRecord(boolean failedToSend, int statusCode)
    throws InterruptedException {
    final var record = new KafkaProducerRecordImpl<>(
      "topic", "key", "value", 10
    );

    final RequestToRecordMapper<String, String> mapper
      = (request, topic) -> Future.succeededFuture(record);

    final KafkaProducer<String, String> producer = mockProducer();

    when(producer.send(any())).thenAnswer(invocationOnMock -> {
      if (failedToSend) {
        return Future.failedFuture("failure");
      } else {
        return Future.succeededFuture();
      }
    });

    final var resource = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .setUid("1-1234")
      .addTopics("1-12345")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
      .build();

    final var request = mock(HttpServerRequest.class);
    when(request.path()).thenReturn(resource.getIngress().getPath());
    final var response = mockResponse(request, statusCode);

    final var handler = new RequestHandler<>(
      new Properties(),
      mapper,
      properties -> producer,
      mock(Counter.class),
      mock(Counter.class)
    );
    final var reconciler = ResourcesReconcilerImpl.builder()
      .watchIngress(handler)
      .build();

    final var countDown = new CountDownLatch(1);

    reconciler.reconcile(List.of(resource))
      .onFailure(cause -> fail())
      .onSuccess(v -> countDown.countDown());

    countDown.await(TIMEOUT, TimeUnit.SECONDS);

    handler.handle(request);

    verifySetStatusCodeAndTerminateResponse(statusCode, response);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void shouldReturnBadRequestIfNoRecordCanBeCreated() throws InterruptedException {
    final var producer = mockProducer();

    final RequestToRecordMapper<Object, Object> mapper
      = (request, topic) -> Future.failedFuture("");

    final var resource = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .setUid("1-1234")
      .addTopics("1-12345")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
      .build();

    final var request = mock(HttpServerRequest.class);
    when(request.path()).thenReturn(resource.getIngress().getPath());
    final var response = mockResponse(request, RequestHandler.MAPPER_FAILED);

    final var handler = new RequestHandler<Object, Object>(
      new Properties(),
      mapper,
      properties -> producer,
      mock(Counter.class),
      mock(Counter.class)
    );
    final var reconciler = ResourcesReconcilerImpl.builder()
      .watchIngress(handler)
      .build();

    final var countDown = new CountDownLatch(1);
    reconciler.reconcile(List.of(resource))
      .onFailure(cause -> fail())
      .onSuccess(v -> countDown.countDown());

    countDown.await(TIMEOUT, TimeUnit.SECONDS);

    handler.handle(request);

    verifySetStatusCodeAndTerminateResponse(RequestHandler.MAPPER_FAILED, response);
  }

  private static void verifySetStatusCodeAndTerminateResponse(
    final int statusCode,
    final HttpServerResponse response) {
    verify(response, times(1)).setStatusCode(statusCode);
    verify(response, times(1)).end();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRecreateProducerWhenBootstrapServerChange(final VertxTestContext context) {
    final RequestToRecordMapper<Object, Object> mapper
      = (request, topic) -> Future.succeededFuture();

    final var first = new AtomicBoolean(true);
    final var recreated = new AtomicBoolean(false);

    final var handler = new RequestHandler<Object, Object>(
      new Properties(),
      mapper,
      properties -> {
        if (!first.getAndSet(false)) {
          recreated.set(true);
        }
        return mockProducer();
      },
      mock(Counter.class),
      mock(Counter.class)
    );
    final var reconciler = ResourcesReconcilerImpl.builder()
      .watchIngress(handler)
      .build();

    final var checkpoint = context.checkpoint();

    final var resource1 = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
      .build();

    final var resource2 = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-3:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
      .build();

    reconciler.reconcile(List.of(resource1))
      .compose(ignored -> reconciler.reconcile(List.of(resource2)))
      .onSuccess(i -> context.verify(() -> {
        assertThat(recreated.get()).isTrue();
        checkpoint.flag();
      }))
      .onFailure(context::failNow);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldNotRecreateProducerWhenBootstrapServerNotChanged(
    final VertxTestContext context) {
    final RequestToRecordMapper<Object, Object> mapper
      = (request, topic) -> Future.succeededFuture();

    final var first = new AtomicBoolean(true);
    final var recreated = new AtomicBoolean(false);

    final var handler = new RequestHandler<Object, Object>(
      new Properties(),
      mapper,
      properties -> {
        if (!first.getAndSet(false)) {
          context.failNow(new IllegalStateException("producer should be recreated"));
        }
        return mockProducer();
      },
      mock(Counter.class),
      mock(Counter.class)
    );
    final var reconciler = ResourcesReconcilerImpl.builder()
      .watchIngress(handler)
      .build();

    final var checkpoint = context.checkpoint();

    final var resource = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
      .build();

    reconciler.reconcile(List.of(resource))
      .compose(ignored -> reconciler.reconcile(List.of(resource)))
      .onSuccess(i -> context.verify(() -> {
        assertThat(recreated.get()).isFalse();
        checkpoint.flag();
      }))
      .onFailure(context::failNow);
  }

  private static KafkaProducer mockProducer() {
    var producer = mock(KafkaProducer.class);
    when(producer.flush()).thenReturn(Future.succeededFuture());
    when(producer.close()).thenReturn(Future.succeededFuture());
    return producer;
  }

  private static HttpServerResponse mockResponse(
    final HttpServerRequest request,
    final int statusCode) {
    final var response = mock(HttpServerResponse.class);
    when(response.setStatusCode(statusCode)).thenReturn(response);
    when(request.response()).thenReturn(response);
    return response;
  }
}
