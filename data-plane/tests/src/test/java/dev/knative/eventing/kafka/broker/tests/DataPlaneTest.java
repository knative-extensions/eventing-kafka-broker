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

package dev.knative.eventing.kafka.broker.tests;

import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import dev.knative.eventing.kafka.broker.core.wrappers.Egress;
import dev.knative.eventing.kafka.broker.core.wrappers.EgressWrapper;
import dev.knative.eventing.kafka.broker.core.wrappers.Resource;
import dev.knative.eventing.kafka.broker.core.wrappers.ResourceWrapper;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import dev.knative.eventing.kafka.broker.dispatcher.ResourcesManager;
import dev.knative.eventing.kafka.broker.dispatcher.http.HttpConsumerVerticleFactory;
import dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper;
import dev.knative.eventing.kafka.broker.receiver.HttpVerticle;
import dev.knative.eventing.kafka.broker.receiver.RequestHandler;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.core.v1.ContextAttributes;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.debezium.kafka.KafkaCluster;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class DataPlaneTest {

  private static final String BROKER_NAMESPACE = "knative-eventing-42";
  private static final String BROKER_NAME = "kafka-broker-42";
  private static final String TOPIC = format("%s-%s", BROKER_NAMESPACE, BROKER_NAME);
  private static final int NUM_BROKERS = 1;
  private static final int ZK_PORT = 2181;
  private static final int KAFKA_PORT = 9092;
  private static final int NUM_PARTITIONS = 10;
  private static final int REPLICATION_FACTOR = 1;
  private static final int INGRESS_PORT = 12345;
  private static final int SERVICE_PORT = INGRESS_PORT + 1;
  private static final int TIMEOUT = 3;

  private static final String TYPE_CE_1 = "type-ce-1";
  private static final String TYPE_CE_2 = "type-ce-2";
  private static final String PATH_SERVICE_1 = "/service-1";
  private static final String PATH_SERVICE_2 = "/service-2";
  private static final String PATH_SERVICE_3 = "/service-3";

  static {
    assertThat(PATH_SERVICE_1).isNotEqualTo(PATH_SERVICE_2);
  }

  private static File dataDir;
  private static KafkaCluster kafkaCluster;
  private static ResourcesReconciler resourcesManager;
  private static ResourcesReconciler handler;

  @BeforeAll
  public static void setUp(final Vertx vertx, final VertxTestContext context)
    throws IOException, InterruptedException {
    setUpKafkaCluster();
    resourcesManager = setUpDispatcher(vertx);
    handler = setUpReceiver(vertx, context);
    context.completeNow();
  }

  /*
  1: event sent by the source to the Broker
  2: event sent by the service in the response
                                                                              2
                                                                  +----------------------+
                                                                  |                      |
                                                                  |                +-----+-----+
                                                                  |          1     |           |
                                                                  |    +---------->+ Trigger 1 |
                                                                  v    |           |           |
  +------------+          +-------------+                 +-------+----+----+      +-----------+
  |            |  1       |             |          2      |                 |
  | HTTPClient +--------->+  Receiver   |        +--------+  Dispatcher     |
  |            |          |             |        |        |                 |
  +------------+          +------+------+        |        +--------+---+----+      +-----------+
                                 |               |                 ^   |           |           |
                                 |               v                 |   +---------->+ Trigger 2 |
                               1 |      +--------+--------+        |         2     |           |
                                 |      |                 |     1  |               +-----------+
                                 +----->+     Kafka       +--------+
                                        |                 |     2                  +-----------+
                                        +-----------------+                        |           |
                                                                                   | Trigger 3 |
                                                                                   |           |
                                                                                   +-----------+
   */
  @Test
  @Timeout(timeUnit = TimeUnit.MINUTES, value = 1)
  public void execute(final Vertx vertx, final VertxTestContext context) throws Exception {

    final var checkpoints = context.checkpoint(3);

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
    final var expectedResponseEvent = CloudEventBuilder.v03()
      .withId(UUID.randomUUID().toString())
      .withDataSchema(URI.create("/api/data-schema-ce-2"))
      .withSubject("subject-ce-2")
      .withSource(URI.create("/api/rossi"))
      .withData("data-ce-2".getBytes())
      .withType(TYPE_CE_2)
      .build();

    final Map<Resource, Set<Egress>> objectsToReconcile = Map
      .of(
        new ResourceWrapper(DataPlaneContract.Resource.newBuilder()
          .addTopics(TOPIC)
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath(format("/%s/%s", BROKER_NAMESPACE, BROKER_NAME)))
          .setBootstrapServers(bootstrapServers())
          .setUid(UUID.randomUUID().toString())
          .build()),
        Set.of(
          new EgressWrapper(DataPlaneContract.Egress.newBuilder()
            .setDestination(format("http://localhost:%d%s", SERVICE_PORT, PATH_SERVICE_1))
            .setFilter(DataPlaneContract.Filter.newBuilder()
              .putAttributes(ContextAttributes.TYPE.name().toLowerCase(), TYPE_CE_1))
            .setConsumerGroup(UUID.randomUUID().toString())
            .build()),
          new EgressWrapper(DataPlaneContract.Egress.newBuilder()
            .setDestination(format("http://localhost:%d%s", SERVICE_PORT, PATH_SERVICE_2))
            .setFilter(DataPlaneContract.Filter.newBuilder()
              .putAttributes(ContextAttributes.TYPE.name().toLowerCase(), TYPE_CE_2))
            .setConsumerGroup(UUID.randomUUID().toString())
            .build()),
          // the destination of the following trigger should never be reached because events
          // don't pass filters.
          new EgressWrapper(DataPlaneContract.Egress.newBuilder()
            .setConsumerGroup(UUID.randomUUID().toString())
            .setDestination(format("http://localhost:%d%s", SERVICE_PORT, PATH_SERVICE_3))
            .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes(
              ContextAttributes.SOURCE.name().toLowerCase(),
              UUID.randomUUID().toString()
            )).build())
        )
      );

    // reconcile resources/egresss
    resourcesManager.reconcile(objectsToReconcile)
      // we don't handle or wait onSuccess, because it's fine to create the consumer at any point
      // in time. (eg before or after starting the destination service, or before or after sending
      // the event to the receiver)
      .onFailure(context::failNow);

    final var waitReconciler = new CountDownLatch(1);

    handler.reconcile(objectsToReconcile)
      .onFailure(context::failNow)
      .onSuccess(v -> waitReconciler.countDown());

    waitReconciler.await(TIMEOUT, TimeUnit.SECONDS);

    // start service
    vertx.createHttpServer()
      .exceptionHandler(context::failNow)
      .requestHandler(request -> VertxMessageFactory
        .createReader(request)
        .map(MessageReader::toEvent)
        .onFailure(context::failNow)
        .onSuccess(event -> {

          // service 1 receives event sent by the HTTPClient
          if (request.path().equals(PATH_SERVICE_1)) {
            context.verify(() -> {
              assertThat(event).isEqualTo(expectedRequestEvent);
              checkpoints.flag(); // 2
            });

            // write event to the response, the event will be handled by service 2
            VertxMessageFactory.createWriter(request.response())
              .writeBinary(expectedResponseEvent);
          }

          // service 2 receives event in the response
          if (request.path().equals(PATH_SERVICE_2)) {
            context.verify(() -> {
              assertThat(event).isEqualTo(expectedResponseEvent);
              checkpoints.flag(); // 3
            });
          }

          if (request.path().equals(PATH_SERVICE_3)) {
            context.failNow(new IllegalStateException(
              PATH_SERVICE_3 + " should never be reached"
            ));
          }
        }))
      .listen(SERVICE_PORT, "localhost")
      .onFailure(context::failNow)
      .onSuccess(ignored -> {
        // send event to the Broker receiver
        VertxMessageFactory.createWriter(
          WebClient.create(vertx)
            .post(INGRESS_PORT, "localhost", format("/%s/%s", BROKER_NAMESPACE, BROKER_NAME))
        ).writeBinary(expectedRequestEvent)
          .onFailure(context::failNow)
          .onSuccess(response -> context.verify(() -> {
            assertThat(response.statusCode())
              .isEqualTo(202);
            checkpoints.flag(); // 1
          }));
      });
  }

  @AfterAll
  public static void teardown() {

    // TODO figure out why shutdown times out Vertx context even with timeout increased.
    // kafkaCluster.shutdown();

    if (!dataDir.delete()) {
      dataDir.deleteOnExit();
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

  private static ResourcesManager setUpDispatcher(final Vertx vertx) {

    final ConsumerRecordOffsetStrategyFactory<String, CloudEvent>
      consumerRecordOffsetStrategyFactory = ConsumerRecordOffsetStrategyFactory.unordered(mock(Counter.class));

    final var consumerConfigs = new Properties();
    consumerConfigs.put(BOOTSTRAP_SERVERS_CONFIG, format("localhost:%d", KAFKA_PORT));
    consumerConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class);

    final var producerConfigs = producerConfigs();

    final var consumerVerticleFactory = new HttpConsumerVerticleFactory(
      consumerRecordOffsetStrategyFactory,
      consumerConfigs,
      WebClient.create(vertx),
      vertx,
      producerConfigs
    );

    return new ResourcesManager(
      vertx,
      consumerVerticleFactory,
      10,
      10
    );
  }

  private static ResourcesReconciler setUpReceiver(
    final Vertx vertx,
    final VertxTestContext context) throws InterruptedException {

    final var handler = new RequestHandler<>(
      producerConfigs(),
      new CloudEventRequestToRecordMapper(),
      properties -> KafkaProducer.create(vertx, properties),
      mock(Counter.class),
      mock(Counter.class)
    );

    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(INGRESS_PORT);

    final var verticle = new HttpVerticle(httpServerOptions, handler);

    final CountDownLatch latch = new CountDownLatch(1);
    vertx.deployVerticle(verticle, context.succeeding(h -> latch.countDown()));
    latch.await();

    return handler;
  }

  private static Properties producerConfigs() {
    final var configs = new Properties();
    configs.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
    return configs;
  }

  private static String bootstrapServers() {
    return format("localhost:%d", KAFKA_PORT);
  }
}
