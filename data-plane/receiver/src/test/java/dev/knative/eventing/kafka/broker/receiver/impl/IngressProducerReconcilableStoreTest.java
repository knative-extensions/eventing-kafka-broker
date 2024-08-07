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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeListerFactory;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.receiver.MockReactiveKafkaProducer;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
                .setIngress(
                        DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
                .build();

        final var resourceUpdated = DataPlaneContract.Resource.newBuilder()
                .setUid("1")
                .addTopics("topic")
                .setBootstrapServers("kafka-1:9092,kafka-3:9092")
                .setIngress(
                        DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
                .build();

        testStore(
                vertx,
                context,
                entry(List.of(resource), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(1);
                }),
                entry(List.of(resourceUpdated), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(2);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(1);
                }),
                entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(2);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(0);
                }));
    }

    @Test
    public void shouldNotRecreateProducerWhenBootstrapServerNotChanged(
            final Vertx vertx, final VertxTestContext context) {

        final var resource = DataPlaneContract.Resource.newBuilder()
                .setUid("1")
                .addTopics("topic")
                .setBootstrapServers("kafka-1:9092,kafka-2:9092")
                .setIngress(
                        DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
                .build();

        testStore(
                vertx,
                context,
                entry(List.of(resource), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(1);
                }),
                entry(List.of(resource), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(1);
                }),
                entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(0);
                }));
    }

    @Test
    public void shouldNotRecreateProducerWhenAddingNewIngressWithSameBootstrapServer(
            final Vertx vertx, final VertxTestContext context) {
        final var resource1 = DataPlaneContract.Resource.newBuilder()
                .setUid("1")
                .addTopics("topic")
                .setBootstrapServers("kafka-1:9092,kafka-2:9092")
                .setIngress(
                        DataPlaneContract.Ingress.newBuilder().setPath("/hello").build())
                .build();

        final var resource2 = DataPlaneContract.Resource.newBuilder()
                .setUid("2")
                .addTopics("topic")
                .setBootstrapServers("kafka-1:9092,kafka-2:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setPath("/hello_world")
                        .build())
                .build();

        testStore(
                vertx,
                context,
                entry(List.of(resource1), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(1);
                }),
                entry(List.of(resource1, resource2), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(1);
                }),
                entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(0);
                }));
    }

    @Test
    public void shouldNotRecreateProducerWhenAddingNewIngressWithSameBootstrapServerAndContentMode(
            final Vertx vertx, final VertxTestContext context) {
        final var resource1 = DataPlaneContract.Resource.newBuilder()
                .setUid("1")
                .addTopics("topic")
                .setBootstrapServers("kafka-1:9092,kafka-2:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setPath("/hello")
                        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                        .build())
                .build();

        final var resource2 = DataPlaneContract.Resource.newBuilder()
                .setUid("2")
                .addTopics("topic")
                .setBootstrapServers("kafka-1:9092,kafka-2:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setPath("/hello_world")
                        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                        .build())
                .build();

        testStore(
                vertx,
                context,
                entry(List.of(resource1, resource2), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(1);
                }),
                entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(1);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(0);
                }));
    }

    @Test
    public void shouldCreateDifferentProducersWhenAddingDifferentIngresses(
            final Vertx vertx, final VertxTestContext context) {
        final var resource1 = DataPlaneContract.Resource.newBuilder()
                .setUid("1")
                .addTopics("topic")
                .setBootstrapServers("kafka-1:9092,kafka-2:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setPath("/hello")
                        .setContentMode(DataPlaneContract.ContentMode.BINARY)
                        .build())
                .build();

        final var resource2 = DataPlaneContract.Resource.newBuilder()
                .setUid("2")
                .addTopics("topic")
                .setBootstrapServers("kafka-3:9092,kafka-4:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setPath("/hello_world")
                        .setContentMode(DataPlaneContract.ContentMode.BINARY)
                        .build())
                .build();

        final var resource2Updated = DataPlaneContract.Resource.newBuilder()
                .setUid("2")
                .addTopics("topic")
                .setBootstrapServers("kafka-3:9092,kafka-4:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setPath("/hello_world")
                        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                        .build())
                .build();

        testStore(
                vertx,
                context,
                entry(List.of(resource1, resource2), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(2);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(2);
                }),
                entry(List.of(resource1, resource2Updated), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(3);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(2);
                }),
                entry(Collections.emptyList(), (producerFactoryInvocations, store) -> {
                    assertThat(producerFactoryInvocations).isEqualTo(3);
                    assertThat(store)
                            .extracting("producerReferences")
                            .asInstanceOf(map(Properties.class, Object.class))
                            .hasSize(0);
                }));
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    private void testStore(
            final Vertx vertx,
            final VertxTestContext context,
            final Map.Entry<List<DataPlaneContract.Resource>, BiConsumer<Integer, IngressProducerReconcilableStore>>...
                    invocations) {
        final var checkpoint = context.checkpoint(invocations.length);

        final var producerFactoryInvocations = new AtomicInteger(0);

        final var store = new IngressProducerReconcilableStore(
                AuthProvider.noAuth(),
                new Properties(),
                properties -> {
                    producerFactoryInvocations.incrementAndGet();
                    return mockProducer();
                },
                mock(EventTypeListerFactory.class));

        final var reconciler = ResourcesReconciler.builder().watchIngress(store).build();

        vertx.runOnContext(v -> {
            Future<Void> fut = Future.succeededFuture();
            for (Map.Entry<List<DataPlaneContract.Resource>, BiConsumer<Integer, IngressProducerReconcilableStore>>
                    entry : invocations) {
                fut = fut.compose(v1 -> reconciler.reconcile(DataPlaneContract.Contract.newBuilder()
                                .addAllResources(entry.getKey())
                                .build()))
                        .onSuccess(i -> context.verify(() -> {
                            entry.getValue().accept(producerFactoryInvocations.get(), store);
                            checkpoint.flag();
                        }))
                        .onFailure(context::failNow);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static ReactiveKafkaProducer<String, CloudEvent> mockProducer() {
        ReactiveKafkaProducer<String, CloudEvent> producer = mock(MockReactiveKafkaProducer.class);
        when(producer.flush()).thenReturn(Future.succeededFuture());
        when(producer.close()).thenReturn(Future.succeededFuture());
        when(producer.unwrap()).thenReturn(new MockProducer<>());
        return producer;
    }

    @Test
    public void shouldResolvePathCorrectly(final Vertx vertx, final VertxTestContext context) {
        final var resource1 = DataPlaneContract.Resource.newBuilder()
                .setUid("1")
                .addTopics("topic1")
                .setBootstrapServers("kafka-1:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setPath("/hello1")
                        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                        .build())
                .build();

        final var resource2 = DataPlaneContract.Resource.newBuilder()
                .setUid("2")
                .addTopics("topic2")
                .setBootstrapServers("kafka-2:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setHost("http://host2")
                        .setPath("/")
                        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                        .build())
                .build();

        final var resource3 = DataPlaneContract.Resource.newBuilder()
                .setUid("3")
                .addTopics("topic3")
                .setBootstrapServers("kafka-3:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setHost("http://host3")
                        .setPath("/hello3")
                        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                        .build())
                .build();

        // additional resource to make sure path "/" don't mean anything
        final var resource4 = DataPlaneContract.Resource.newBuilder()
                .setUid("4")
                .addTopics("topic4")
                .setBootstrapServers("kafka-4:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setHost("http://host4")
                        .setPath("/")
                        .setContentMode(DataPlaneContract.ContentMode.STRUCTURED)
                        .build())
                .build();

        ReactiveKafkaProducer<String, CloudEvent> producer1 = mockProducer();
        ReactiveKafkaProducer<String, CloudEvent> producer2 = mockProducer();
        ReactiveKafkaProducer<String, CloudEvent> producer3 = mockProducer();
        ReactiveKafkaProducer<String, CloudEvent> producer4 = mockProducer();

        Map<String, ReactiveKafkaProducer<String, CloudEvent>> producerMap = Map.of(
                "kafka-1:9092", producer1,
                "kafka-2:9092", producer2,
                "kafka-3:9092", producer3,
                "kafka-4:9092", producer4);

        final var store = new IngressProducerReconcilableStore(
                AuthProvider.noAuth(),
                new Properties(),
                properties -> {
                    ReactiveKafkaProducer<String, CloudEvent> producer =
                            producerMap.get(properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
                    if (producer == null) {
                        throw new IllegalStateException("Can't determine what producer to return");
                    }
                    return producer;
                },
                mock(EventTypeListerFactory.class));

        final var reconciler = ResourcesReconciler.builder().watchIngress(store).build();

        vertx.runOnContext(v -> {
            Future.succeededFuture()
                    .compose(v1 -> reconciler.reconcile(DataPlaneContract.Contract.newBuilder()
                            .addAllResources(List.of(resource1, resource2, resource3, resource4))
                            .build()))
                    .onSuccess(event -> {
                        // match by path
                        assertThat(store.resolve("", "/hello1").getKafkaProducer())
                                .isSameAs(producer1);
                        assertThat(store.resolve("", "/hello3").getKafkaProducer())
                                .isSameAs(producer3);

                        // match by host
                        assertThat(store.resolve("http://host2", "/").getKafkaProducer())
                                .isSameAs(producer2);
                        assertThat(store.resolve("http://host4", "/").getKafkaProducer())
                                .isSameAs(producer4);
                        assertThat(store.resolve("http://host2", "").getKafkaProducer())
                                .isSameAs(producer2);
                        assertThat(store.resolve("http://host4", "").getKafkaProducer())
                                .isSameAs(producer4);

                        // only use path when the path is registered
                        assertThat(store.resolve("http://host1", "/hello1").getKafkaProducer())
                                .isSameAs(producer1);
                        assertThat(store.resolve("http://host1", "/hello1/").getKafkaProducer())
                                .isSameAs(producer1);
                        assertThat(store.resolve("http://host2", "/hello1").getKafkaProducer())
                                .isSameAs(producer1);
                        assertThat(store.resolve("http://host1", "/hello3").getKafkaProducer())
                                .isSameAs(producer3);
                        assertThat(store.resolve("http://host2", "/hello3").getKafkaProducer())
                                .isSameAs(producer3);
                        assertThat(store.resolve("http://host3", "/hello3").getKafkaProducer())
                                .isSameAs(producer3);

                        // match the host when the path is not registered
                        assertThat(store.resolve("http://host2", "/doesntExist").getKafkaProducer())
                                .isSameAs(producer2);
                        assertThat(store.resolve("http://host3", "/doesntExist").getKafkaProducer())
                                .isSameAs(producer3);
                        assertThat(store.resolve("http://host4", "/doesntExist").getKafkaProducer())
                                .isSameAs(producer4);

                        // don't return anything when there's nothing matching
                        assertThat(store.resolve("http://unknown", "/")).isNull();
                        assertThat(store.resolve("http://unknown", "")).isNull();
                        assertThat(store.resolve("", "/doesntExist")).isNull();
                        assertThat(store.resolve("http://unknown", "/doesntExist"))
                                .isNull();

                        context.completeNow();
                    })
                    .onFailure(context::failNow);
        });
    }

    @Test
    public void shouldRejectIngressWithRootPathAndEmptyHost(final Vertx vertx, final VertxTestContext context) {
        final var resource = DataPlaneContract.Resource.newBuilder()
                .setUid("1")
                .addTopics("topic")
                .setBootstrapServers("kafka-1:9092,kafka-2:9092")
                .setIngress(DataPlaneContract.Ingress.newBuilder()
                        .setHost("")
                        .setPath("/")
                        .build())
                .build();

        final var store = new IngressProducerReconcilableStore(
                AuthProvider.noAuth(),
                new Properties(),
                properties -> mockProducer(),
                mock(EventTypeListerFactory.class));

        final var reconciler = ResourcesReconciler.builder().watchIngress(store).build();

        vertx.runOnContext(v -> {
            Future.succeededFuture()
                    .compose(v1 -> reconciler.reconcile(DataPlaneContract.Contract.newBuilder()
                            .addAllResources(List.of(resource))
                            .build()))
                    .onSuccess(event -> context.failNow("Reconcile should've failed"))
                    .onFailure(event -> context.completeNow());
        });
    }
}
