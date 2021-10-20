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
package dev.knative.eventing.kafka.broker.receiver.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class IngressProducerReconcilableStoreTest {

  static {
    BackendRegistries.setupBackend(new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME));
  }

  @Test
  public void shouldRecreateProducerWhenBootstrapServerChange(final Vertx vertx, final VertxTestContext context) {
    final var resource = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
      .build();

    final var resourceUpdated = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-3:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
      .build();

    testStore(vertx, context,
      entry(List.of(resource), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(1);
      }),
      entry(List.of(resourceUpdated), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(2);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(1);
      }),
      entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(2);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(0);
      })
    );
  }

  @Test
  public void shouldNotRecreateProducerWhenBootstrapServerNotChanged(
    final Vertx vertx,
    final VertxTestContext context) {

    final var resource = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
      .build();

    testStore(vertx, context,
      entry(List.of(resource), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(1);
      }),
      entry(List.of(resource), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(1);
      }),
      entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(0);
      })
    );
  }

  @Test
  public void shouldNotRecreateProducerWhenAddingNewIngressWithSameBootstrapServer(
    final Vertx vertx,
    final VertxTestContext context) {
    final var resource1 = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
      .build();

    final var resource2 = DataPlaneContract.Resource.newBuilder()
      .setUid("2")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello_world").build())
      .build();

    testStore(vertx, context,
      entry(List.of(resource1), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(1);
      }),
      entry(List.of(resource1, resource2), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(1);
      }),
      entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(0);
      })
    );
  }

  @Test
  public void shouldNotRecreateProducerWhenAddingNewIngressWithSameBootstrapServerAndContentMode(
    final Vertx vertx,
    final VertxTestContext context) {
    final var resource1 = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello")
        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED).build())
      .build();

    final var resource2 = DataPlaneContract.Resource.newBuilder()
      .setUid("2")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello_world")
        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED).build())
      .build();

    testStore(vertx, context,
      entry(List.of(resource1, resource2), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(1);
      }),
      entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(1);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(0);
      })
    );
  }

  @Test
  public void shouldCreateDifferentProducersWhenAddingDifferentIngresses(
    final Vertx vertx,
    final VertxTestContext context) {
    final var resource1 = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic")
      .setBootstrapServers("kafka-1:9092,kafka-2:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello")
        .setContentMode(DataPlaneContract.ContentMode.BINARY).build())
      .build();

    final var resource2 = DataPlaneContract.Resource.newBuilder()
      .setUid("2")
      .addTopics("topic")
      .setBootstrapServers("kafka-3:9092,kafka-4:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello_world")
        .setContentMode(DataPlaneContract.ContentMode.BINARY).build())
      .build();

    final var resource2Updated = DataPlaneContract.Resource.newBuilder()
      .setUid("2")
      .addTopics("topic")
      .setBootstrapServers("kafka-3:9092,kafka-4:9092")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/hello_world")
        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED).build())
      .build();

    testStore(vertx, context,
      entry(List.of(resource1, resource2), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(2);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(2);
      }),
      entry(List.of(resource1, resource2Updated), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(3);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(2);
      }),
      entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
        assertThat(producerFactoryInvocations)
          .isEqualTo(3);
        assertThat(store)
          .extracting("producerReferences")
          .asInstanceOf(map(Properties.class, Object.class))
          .hasSize(0);
      })
    );
  }

  @SafeVarargs
  @SuppressWarnings("unchecked")
  private void testStore(
    final Vertx vertx,
    final VertxTestContext context,
    final Map.Entry<List<DataPlaneContract.Resource>, BiConsumer<Integer, IngressProducerReconcilableStore>>... invocations) {
    final var checkpoint = context.checkpoint(invocations.length);

    final var producerFactoryInvocations = new AtomicInteger(0);

    final var store = new IngressProducerReconcilableStore(
      AuthProvider.noAuth(),
      new Properties(),
      properties -> {
        producerFactoryInvocations.incrementAndGet();
        return mockProducer();
      }
    );

    final var reconciler = ResourcesReconciler
      .builder()
      .watchIngress(store)
      .build();

    vertx.runOnContext(v -> {
      Future<Void> fut = Future.succeededFuture();
      for (Map.Entry<List<DataPlaneContract.Resource>, BiConsumer<Integer, IngressProducerReconcilableStore>> entry : invocations) {
        fut = fut.compose(v1 -> reconciler.reconcile(entry.getKey()))
          .onSuccess(i -> context.verify(() -> {
            entry.getValue().accept(producerFactoryInvocations.get(), store);
            checkpoint.flag();
          }))
          .onFailure(context::failNow);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private static KafkaProducer<String, CloudEvent> mockProducer() {
    KafkaProducer<String, CloudEvent> producer = mock(KafkaProducer.class);
    when(producer.flush()).thenReturn(Future.succeededFuture());
    when(producer.close()).thenReturn(Future.succeededFuture());
    when(producer.unwrap()).thenReturn(new MockProducer<>());
    return producer;
  }
}
