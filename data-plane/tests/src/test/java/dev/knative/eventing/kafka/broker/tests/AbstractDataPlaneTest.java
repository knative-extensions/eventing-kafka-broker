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
package dev.knative.eventing.kafka.broker.tests;

import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.ReactiveConsumerFactory;
import dev.knative.eventing.kafka.broker.core.ReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeListerFactory;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.CloudEventDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEventInterceptor;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.KeyDeserializer;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.NullCloudEventInterceptor;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerDeployerVerticle;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleFactoryImpl;
import dev.knative.eventing.kafka.broker.receiver.impl.IngressProducerReconcilableStore;
import dev.knative.eventing.kafka.broker.receiver.impl.ReceiverVerticle;
import dev.knative.eventing.kafka.broker.receiver.impl.StrictRequestToRecordMapper;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.IngressRequestHandlerImpl;
import dev.knative.eventing.kafka.broker.receiver.main.ReceiverEnv;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.core.v1.CloudEventV1;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.kafka.CloudEventSerializer;
import io.debezium.kafka.KafkaCluster;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public abstract class AbstractDataPlaneTest {

    private static final String BROKER_NAMESPACE = "knative-eventing-42";
    private static final String BROKER_NAME = "kafka-broker-42";
    private static final String TOPIC = format("%s-%s", BROKER_NAMESPACE, BROKER_NAME);
    private static final int NUM_BROKERS = 1;
    private static final int ZK_PORT = 2181;
    private static final int KAFKA_PORT = 9092;
    private static final int NUM_PARTITIONS = 10;
    private static final int REPLICATION_FACTOR = 1;
    private static final int INGRESS_PORT = 12345;
    private static final int INGRESS_TLS_PORT = 12343;
    private static final int SERVICE_PORT = INGRESS_PORT + 1;
    private static final int NUM_SYSTEM_VERTICLES = 1;
    private static final int NUM_RESOURCES = 1;

    private static final String TYPE_CE_1 = "type-ce-1";
    private static final String TYPE_CE_2 = "type-ce-2";
    private static final String PATH_SERVICE_1 = "/service-1";
    private static final String PATH_SERVICE_2 = "/service-2";
    private static final String PATH_SERVICE_3 = "/service-3";

    private static final String SECRET_VOLUME_PATH = "src/test/resources";

    static {
        assertThat(PATH_SERVICE_1).isNotEqualTo(PATH_SERVICE_2);
        BackendRegistries.setupBackend(new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME));
    }

    private static File dataDir;
    private static KafkaCluster kafkaCluster;
    private static ConsumerDeployerVerticle consumerDeployerVerticle;
    private static ReceiverVerticle receiverVerticle;

    protected abstract ReactiveProducerFactory getReactiveProducerFactory();

    protected abstract ReactiveConsumerFactory getReactiveConsumerFactory();

    @BeforeEach
    public void setUp(final Vertx vertx, final VertxTestContext context) throws IOException, InterruptedException {
        setUpKafkaCluster();
        ContractMessageCodec.register(vertx.eventBus());
        consumerDeployerVerticle = setUpDispatcher(vertx, context);
        receiverVerticle = setUpReceiver(vertx, context);
        context.completeNow();
    }

    /*
    1: event sent by the source to the Broker
    2: event sent by the trigger 1 in the response
    3: event sent by the trigger 2 in the response
                                                                                2
                                                                    +----------------------+
                                                                    |                      |
                                                                    |                +-----+-----+
                                                                    |          1     |           |
                                                                    |    +---------->+ Trigger 1 |
                                                                    v    |     3     |           |
    +------------+          +-------------+                 +-------+----+----+      +-----------+
    |            |  1       |             |          2      |                 |
    | HTTPClient +--------->+  Receiver   |        +--------+  Dispatcher     |
    |            |          |             |        |        |                 |
    +------------+          +------+------+        |        +--------+---+----+      +-----------+
                                   |               |                 ^   | 3         |           |
                                   |               v                 |   +---------->+ Trigger 2 |
                                 1 |      +--------+--------+        |         2     |           |
                                   |      |                 |     1  |               +-----------+
                                   +----->+     Kafka       +--------+
                                          |                 |     2                  +-----------+
                                          +-----------------+     3                  |           |
                                                                                     | Trigger 3 |
                                                                                     |           |
                                                                                     +-----------+




     */
    @Test
    @Timeout(timeUnit = TimeUnit.MINUTES, value = 5)
    public void execute(final Vertx vertx, final VertxTestContext context) throws InterruptedException {

        final var checkpoints = context.checkpoint(4);

        // event sent by the source to the Broker (see 1 in diagram)
        final var expectedRequestEvent = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withDataSchema(URI.create("/api/data-schema-ce-1"))
                .withSource(URI.create("/api/rossi"))
                .withSubject("subject-ce-1")
                .withData("data-ce-1".getBytes())
                .withType(TYPE_CE_1)
                .build();

        // event sent in the response by the Callable service (see 2 in diagram)
        final var expectedResponseEventService2 = CloudEventBuilder.v03()
                .withId(UUID.randomUUID().toString())
                .withDataSchema(URI.create("/api/data-schema-ce-2"))
                .withSubject("subject-ce-2")
                .withSource(URI.create("/api/rossi"))
                .withData("data-ce-2".getBytes())
                .withType(TYPE_CE_2)
                .build();

        // event sent in the response by the Callable service 2 (see 3 in diagram)
        final var expectedResponseEventService1 = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withDataSchema(URI.create("/api/data-schema-ce-3"))
                .withSource(URI.create("/api/rossi"))
                .withSubject("subject-ce-3")
                .withType(TYPE_CE_1)
                .build();

        final var service1ExpectedEventsIterator =
                List.of(expectedRequestEvent, expectedResponseEventService1).iterator();

        final var resource = DataPlaneContract.Resource.newBuilder()
                .addTopics(TOPIC)
                .setIngress(
                        DataPlaneContract.Ingress.newBuilder().setPath(format("/%s/%s", BROKER_NAMESPACE, BROKER_NAME)))
                .setBootstrapServers(bootstrapServers())
                .setUid(UUID.randomUUID().toString())
                .addEgresses(DataPlaneContract.Egress.newBuilder()
                        .setUid(UUID.randomUUID().toString())
                        .setDestination(format("http://localhost:%d%s", SERVICE_PORT, PATH_SERVICE_1))
                        .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes(CloudEventV1.TYPE, TYPE_CE_1))
                        .setConsumerGroup(UUID.randomUUID().toString())
                        .build())
                .addEgresses(DataPlaneContract.Egress.newBuilder()
                        .setUid(UUID.randomUUID().toString())
                        .setDestination(format("http://localhost:%d%s", SERVICE_PORT, PATH_SERVICE_2))
                        .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes(CloudEventV1.TYPE, TYPE_CE_2))
                        .setConsumerGroup(UUID.randomUUID().toString())
                        .build())
                .addEgresses(
                        // the destination of the following trigger should never be reached because events
                        // don't pass filters.
                        DataPlaneContract.Egress.newBuilder()
                                .setUid(UUID.randomUUID().toString())
                                .setConsumerGroup(UUID.randomUUID().toString())
                                .setDestination(format("http://localhost:%d%s", SERVICE_PORT, PATH_SERVICE_3))
                                .setFilter(DataPlaneContract.Filter.newBuilder()
                                        .putAttributes(
                                                CloudEventV1.SOURCE,
                                                UUID.randomUUID().toString()))
                                .build())
                .build();

        new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS)
                .accept(DataPlaneContract.Contract.newBuilder()
                        .addResources(resource)
                        .build());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(vertx.deploymentIDs())
                .hasSize(resource.getEgressesCount() + NUM_RESOURCES + NUM_SYSTEM_VERTICLES));

        Thread.sleep(2000); // Give consumers time to start

        // start service
        vertx.createHttpServer()
                .exceptionHandler(context::failNow)
                .requestHandler(request -> VertxMessageFactory.createReader(request)
                        .map(MessageReader::toEvent)
                        .onFailure(context::failNow)
                        .onSuccess(event -> {

                            // service 1 receives event sent by the HTTPClient
                            if (request.path().equals(PATH_SERVICE_1)) {
                                final var expectedEvent = service1ExpectedEventsIterator.next();
                                context.verify(() -> {
                                    assertThat(event.getId()).isEqualTo(expectedEvent.getId());
                                    assertThat(event.getType()).isEqualTo(expectedEvent.getType());
                                    assertThat(event.getSubject()).isEqualTo(expectedEvent.getSubject());
                                    assertThat(event.getSource()).isEqualTo(expectedEvent.getSource());
                                    checkpoints.flag(); // 2
                                });

                                if (service1ExpectedEventsIterator.hasNext()) {
                                    // write event to the response, the event will be handled by service 2
                                    VertxMessageFactory.createWriter(request.response())
                                            .writeBinary(expectedResponseEventService2);
                                }
                            }

                            // service 2 receives event in the response
                            if (request.path().equals(PATH_SERVICE_2)) {
                                context.verify(() -> {
                                    assertThat(event.getId()).isEqualTo(expectedResponseEventService2.getId());
                                    assertThat(event.getType()).isEqualTo(expectedResponseEventService2.getType());
                                    assertThat(event.getSubject())
                                            .isEqualTo(expectedResponseEventService2.getSubject());
                                    assertThat(event.getSource()).isEqualTo(expectedResponseEventService2.getSource());
                                    checkpoints.flag(); // 3
                                });

                                // write event to the response, the event will be handled by service 2
                                VertxMessageFactory.createWriter(request.response())
                                        .writeBinary(expectedResponseEventService1);
                            }

                            if (request.path().equals(PATH_SERVICE_3)) {
                                context.failNow(new IllegalStateException(PATH_SERVICE_3 + " should never be reached"));
                            }
                        }))
                .listen(SERVICE_PORT, "localhost")
                .onFailure(context::failNow)
                .onSuccess(ignored -> {
                    // send event to the Broker receiver
                    VertxMessageFactory.createWriter(WebClient.create(vertx)
                                    .post(INGRESS_PORT, "localhost", format("/%s/%s", BROKER_NAMESPACE, BROKER_NAME)))
                            .writeBinary(expectedRequestEvent)
                            .onFailure(context::failNow)
                            .onSuccess(response -> context.verify(() -> {
                                assertThat(response.statusCode()).isEqualTo(202);
                                checkpoints.flag(); // 1
                            }));
                });
    }

    @AfterEach
    public void teardown(final Vertx vertx) throws ExecutionException, InterruptedException {

        vertx.undeploy(consumerDeployerVerticle.deploymentID())
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        vertx.undeploy(receiverVerticle.deploymentID())
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        teardownKafkaCluster();
    }

    private static void teardownKafkaCluster() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
            kafkaCluster = null;
            if (!dataDir.delete()) {
                dataDir.deleteOnExit();
            }
        }
    }

    private static void setUpKafkaCluster() throws IOException {

        dataDir = File.createTempFile("kafka", "kafka");

        kafkaCluster = new KafkaCluster()
                .withPorts(ZK_PORT, KAFKA_PORT)
                .deleteDataPriorToStartup(true)
                .addBrokers(NUM_BROKERS)
                .usingDirectory(dataDir)
                .startup();

        kafkaCluster.createTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
    }

    private ConsumerDeployerVerticle setUpDispatcher(final Vertx vertx, final VertxTestContext context)
            throws InterruptedException {

        final var consumerConfigs = new Properties();
        consumerConfigs.put(BOOTSTRAP_SERVERS_CONFIG, format("localhost:%d", KAFKA_PORT));
        consumerConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, KeyDeserializer.class.getName());
        consumerConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        consumerConfigs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        consumerConfigs.put(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                NullCloudEventInterceptor.class.getName() + "," + InvalidCloudEventInterceptor.class.getName());

        final var producerConfigs = producerConfigs();

        final var consumerVerticleFactory = new ConsumerVerticleFactoryImpl(
                consumerConfigs,
                new WebClientOptions(),
                producerConfigs,
                AuthProvider.noAuth(),
                Metrics.getRegistry(),
                getReactiveConsumerFactory(),
                getReactiveProducerFactory());

        final var verticle = new ConsumerDeployerVerticle(consumerVerticleFactory, 10);

        final CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle(verticle, context.succeeding(h -> latch.countDown()));
        latch.await();

        return verticle;
    }

    private ReceiverVerticle setUpReceiver(final Vertx vertx, final VertxTestContext context)
            throws InterruptedException {

        final var httpServerOptions = new HttpServerOptions();
        httpServerOptions.setPort(INGRESS_PORT);

        final var httpsServerOptions = new HttpServerOptions();
        httpsServerOptions.setPort(INGRESS_TLS_PORT);
        httpsServerOptions.setSsl(true);

        final var env = mock(ReceiverEnv.class);
        when(env.getLivenessProbePath()).thenReturn("/healthz");
        when(env.getReadinessProbePath()).thenReturn("/readyz");

        final var verticle = new ReceiverVerticle(
                env,
                httpServerOptions,
                httpsServerOptions,
                v -> new IngressProducerReconcilableStore(
                        AuthProvider.noAuth(),
                        producerConfigs(),
                        properties -> getReactiveProducerFactory().create(v, properties),
                        mock(EventTypeListerFactory.class)),
                new IngressRequestHandlerImpl(
                        StrictRequestToRecordMapper.getInstance(),
                        Metrics.getRegistry(),
                        (((event, lister, reference) -> null))),
                SECRET_VOLUME_PATH,
                null);

        final CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle(verticle, context.succeeding(h -> latch.countDown()));
        latch.await();

        return verticle;
    }

    private static Properties producerConfigs() {
        final var configs = new Properties();
        configs.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());
        return configs;
    }

    private static String bootstrapServers() {
        return format("localhost:%d", KAFKA_PORT);
    }
}
