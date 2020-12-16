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
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Resource;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerImpl;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class RequestMapperTest {

  private static final int TIMEOUT = 3;

  static {
    BackendRegistries.setupBackend(
        new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME));
  }

  @Test
  public void shouldSendRecordAndTerminateRequestWithRecordProduced() throws InterruptedException {
    shouldSendRecord(false, RequestMapper.RECORD_PRODUCED);
  }

  @Test
  public void shouldSendRecordAndTerminateRequestWithFailedToProduce() throws InterruptedException {
    shouldSendRecord(true, RequestMapper.FAILED_TO_PRODUCE);
  }

  @SuppressWarnings("unchecked")
  private static void shouldSendRecord(final boolean failedToSend, final int statusCode)
      throws InterruptedException {
    final var record = new KafkaProducerRecordImpl<>("topic", "key", "value", 10);

    final RequestToRecordMapper<String, String> mapper =
        (request, topic) -> Future.succeededFuture(record);

    final KafkaProducer<String, String> producer = mockProducer();

    when(producer.send(any()))
        .thenAnswer(
            invocationOnMock -> {
              if (failedToSend) {
                return Future.failedFuture("failure");
              } else {
                return Future.succeededFuture(mock(RecordMetadata.class));
              }
            });

    final var resource =
        DataPlaneContract.Resource.newBuilder()
            .setUid("1")
            .setUid("1-1234")
            .addTopics("1-12345")
            .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
            .build();

    final HttpServerRequest request = mockHttpServerRequest(resource);
    final var response = mockResponse(request, statusCode);

    final var handler =
        new RequestMapper<>(
            mock(Vertx.class),
            new Properties(),
            mapper,
            properties -> producer,
            mock(Counter.class),
            mock(Counter.class));
    final var reconciler = ResourcesReconcilerImpl.builder().watchIngress(handler).build();

    final var countDown = new CountDownLatch(1);

    reconciler
        .reconcile(List.of(resource))
        .onFailure(cause -> fail())
        .onSuccess(v -> countDown.countDown());

    countDown.await(TIMEOUT, TimeUnit.SECONDS);

    handler.handle(request);

    verifySetStatusCodeAndTerminateResponse(statusCode, response);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnBadRequestIfNoRecordCanBeCreated(final Vertx vertx)
      throws InterruptedException {
    final var producer = mockProducer();

    final RequestToRecordMapper<Object, Object> mapper =
        (request, topic) -> Future.failedFuture("");

    final var resource =
        DataPlaneContract.Resource.newBuilder()
            .setUid("1")
            .setUid("1-1234")
            .addTopics("1-12345")
            .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello"))
            .build();

    final HttpServerRequest request = mockHttpServerRequest(resource);
    final var response = mockResponse(request, RequestMapper.MAPPER_FAILED);

    final var handler =
        new RequestMapper<Object, Object>(
            vertx,
            new Properties(),
            mapper,
            properties -> producer,
            mock(Counter.class),
            mock(Counter.class));
    final var reconciler = ResourcesReconcilerImpl.builder().watchIngress(handler).build();

    final var countDown = new CountDownLatch(1);
    reconciler
        .reconcile(List.of(resource))
        .onFailure(cause -> fail())
        .onSuccess(v -> countDown.countDown());

    countDown.await(TIMEOUT, TimeUnit.SECONDS);

    handler.handle(request);

    verifySetStatusCodeAndTerminateResponse(RequestMapper.MAPPER_FAILED, response);
  }

  private static void verifySetStatusCodeAndTerminateResponse(
      final int statusCode, final HttpServerResponse response) {
    verify(response, times(1)).setStatusCode(statusCode);
    verify(response, times(1)).end();
  }

  @Test
  public void shouldRecreateProducerWhenBootstrapServerChange(
      final Vertx vertx, final VertxTestContext context) {
    final var resource =
        DataPlaneContract.Resource.newBuilder()
            .setUid("1")
            .addTopics("topic")
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
            .build();

    final var resourceUpdated =
        DataPlaneContract.Resource.newBuilder()
            .setUid("1")
            .addTopics("topic")
            .setBootstrapServers("kafka-1:9092,kafka-3:9092")
            .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
            .build();

    testRequestMapper(
        vertx,
        context,
        entry(
            List.of(resource),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(1);
            }),
        entry(
            List.of(resourceUpdated),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(2);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(1);
            }),
        entry(
            Collections.emptyList(),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(2);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(0);
            }));
  }

  @Test
  public void shouldNotRecreateProducerWhenBootstrapServerNotChanged(
      final Vertx vertx, final VertxTestContext context) {

    final var resource =
        DataPlaneContract.Resource.newBuilder()
            .setUid("1")
            .addTopics("topic")
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
            .build();

    testRequestMapper(
        vertx,
        context,
        entry(
            List.of(resource),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(1);
            }),
        entry(
            List.of(resource),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(1);
            }),
        entry(
            Collections.emptyList(),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(0);
            }));
  }

  @Test
  public void shouldNotRecreateProducerWhenAddingNewIngressWithSameBootstrapServer(
      final Vertx vertx, final VertxTestContext context) {
    final var resource1 =
        DataPlaneContract.Resource.newBuilder()
            .setUid("1")
            .addTopics("topic")
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
            .build();

    final var resource2 =
        DataPlaneContract.Resource.newBuilder()
            .setUid("2")
            .addTopics("topic")
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello_world").build())
            .build();

    testRequestMapper(
        vertx,
        context,
        entry(
            List.of(resource1),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(1);
            }),
        entry(
            List.of(resource1, resource2),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(1);
            }),
        entry(
            Collections.emptyList(),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(0);
            }));
  }

  @Test
  public void shouldNotRecreateProducerWhenAddingNewIngressWithSameBootstrapServerAndContentMode(
      final Vertx vertx, final VertxTestContext context) {
    final var resource1 =
        DataPlaneContract.Resource.newBuilder()
            .setUid("1")
            .addTopics("topic")
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .setIngress(
                DataPlaneContract.Ingress.newBuilder()
                    .setPath("/hello")
                    .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                    .build())
            .build();

    final var resource2 =
        DataPlaneContract.Resource.newBuilder()
            .setUid("2")
            .addTopics("topic")
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .setIngress(
                DataPlaneContract.Ingress.newBuilder()
                    .setPath("/hello_world")
                    .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                    .build())
            .build();

    testRequestMapper(
        vertx,
        context,
        entry(
            List.of(resource1, resource2),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(1);
            }),
        entry(
            Collections.emptyList(),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(1);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(0);
            }));
  }

  @Test
  public void shouldCreateDifferentProducersWhenAddingDifferentIngresses(
      final Vertx vertx, final VertxTestContext context) {
    final var resource1 =
        DataPlaneContract.Resource.newBuilder()
            .setUid("1")
            .addTopics("topic")
            .setBootstrapServers("kafka-1:9092,kafka-2:9092")
            .setIngress(
                DataPlaneContract.Ingress.newBuilder()
                    .setPath("/hello")
                    .setContentMode(DataPlaneContract.ContentMode.BINARY)
                    .build())
            .build();

    final var resource2 =
        DataPlaneContract.Resource.newBuilder()
            .setUid("2")
            .addTopics("topic")
            .setBootstrapServers("kafka-3:9092,kafka-4:9092")
            .setIngress(
                DataPlaneContract.Ingress.newBuilder()
                    .setPath("/hello_world")
                    .setContentMode(DataPlaneContract.ContentMode.BINARY)
                    .build())
            .build();

    final var resource2Updated =
        DataPlaneContract.Resource.newBuilder()
            .setUid("2")
            .addTopics("topic")
            .setBootstrapServers("kafka-3:9092,kafka-4:9092")
            .setIngress(
                DataPlaneContract.Ingress.newBuilder()
                    .setPath("/hello_world")
                    .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                    .build())
            .build();

    testRequestMapper(
        vertx,
        context,
        entry(
            List.of(resource1, resource2),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(2);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(2);
            }),
        entry(
            List.of(resource1, resource2Updated),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(3);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(2);
            }),
        entry(
            Collections.emptyList(),
            (producerFactoryInvocations, mapper) -> {
              assertThat(producerFactoryInvocations).isEqualTo(3);
              assertThat(mapper)
                  .extracting("producerReferences")
                  .asInstanceOf(map(Properties.class, Object.class))
                  .hasSize(0);
            }));
  }

  @SafeVarargs
  @SuppressWarnings("unchecked")
  private void testRequestMapper(
      final Vertx vertx,
      final VertxTestContext context,
      final Map.Entry<
              List<DataPlaneContract.Resource>, BiConsumer<Integer, RequestMapper<Object, Object>>>
              ...
          invocations) {
    final var checkpoint = context.checkpoint(invocations.length);

    final var producerFactoryInvocations = new AtomicInteger(0);

    final var handler =
        new RequestMapper<Object, Object>(
            vertx,
            new Properties(),
            (request, topic) -> Future.succeededFuture(),
            properties -> {
              producerFactoryInvocations.incrementAndGet();
              return mockProducer();
            },
            mock(Counter.class),
            mock(Counter.class));
    final var reconciler = ResourcesReconcilerImpl.builder().watchIngress(handler).build();

    Future<Void> fut = Future.succeededFuture();
    for (final Map.Entry<
            List<DataPlaneContract.Resource>, BiConsumer<Integer, RequestMapper<Object, Object>>>
        entry : invocations) {
      fut =
          fut.compose(v -> reconciler.reconcile(entry.getKey()))
              .onSuccess(
                  i ->
                      context.verify(
                          () -> {
                            entry.getValue().accept(producerFactoryInvocations.get(), handler);
                            checkpoint.flag();
                          }))
              .onFailure(context::failNow);
    }
  }

  @SuppressWarnings("rawtypes")
  private static KafkaProducer mockProducer() {
    final var producer = mock(KafkaProducer.class);
    when(producer.flush()).thenReturn(Future.succeededFuture());
    when(producer.close()).thenReturn(Future.succeededFuture());
    when(producer.unwrap()).thenReturn(new MockProducer());
    return producer;
  }

  private static HttpServerRequest mockHttpServerRequest(final Resource resource) {
    final var request = mock(HttpServerRequest.class);
    when(request.path()).thenReturn(resource.getIngress().getPath());
    when(request.method()).thenReturn(new HttpMethod("POST"));
    when(request.host()).thenReturn("127.0.0.1");
    when(request.scheme()).thenReturn("http");
    when(request.headers()).thenReturn(new HeadersMultiMap());
    return request;
  }

  private static HttpServerResponse mockResponse(
      final HttpServerRequest request, final int statusCode) {
    final var response = mock(HttpServerResponse.class);
    when(response.setStatusCode(statusCode)).thenReturn(response);
    when(request.response()).thenReturn(response);
    return response;
  }
}
