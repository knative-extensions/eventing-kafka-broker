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

import static dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper.TOPIC_PREFIX;
import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.BrokerWrapper;
import dev.knative.eventing.kafka.broker.core.TriggerWrapper;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Trigger;
import dev.knative.eventing.kafka.broker.dispatcher.BrokersManager;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
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
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
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
import org.apache.kafka.clients.producer.ProducerConfig;
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
  private static final String TOPIC =
      format("%s%s-%s", TOPIC_PREFIX, BROKER_NAMESPACE, BROKER_NAME);
  private static final int NUM_BROKERS = 1;
  private static final int ZK_PORT = 2181;
  private static final int KAFKA_PORT = 9092;
  private static final int NUM_PARTITIONS = 10;
  private static final int REPLICATION_FACTOR = 1;
  private static final int INGRESS_PORT = 12345;
  private static final int SERVICE_PORT = INGRESS_PORT + 1;

  private static final String TYPE_CE_1 = "type-ce-1";
  private static final String TYPE_CE_2 = "type-ce-2";
  private static final String PATH_SERVICE_1 = "/service-1";
  private static final String PATH_SERVICE_2 = "/service-2";

  static {
    assertThat(PATH_SERVICE_1).isNotEqualTo(PATH_SERVICE_2);
  }

  private static File dataDir;
  private static KafkaCluster kafkaCluster;
  private static BrokersManager<CloudEvent> brokersManager;

  @BeforeAll
  public static void setUp(final Vertx vertx, final VertxTestContext context) throws IOException {
    setUpKafkaCluster();
    setUpReceiver(vertx, context);
    setUpDispatcher(vertx);
  }

  @Test
  public void execute(final Vertx vertx, final VertxTestContext context) {

    final var checkpoints = context.checkpoint(3);

    // event written by the source to the Broker
    final var requestEvent = CloudEventBuilder.v1()
        .withId(UUID.randomUUID().toString())
        .withDataSchema(URI.create("/api/data-schema-ce-1"))
        .withSource(URI.create("/api/rossi"))
        .withSubject("subject-ce-1")
        .withData("data-ce-1".getBytes())
        .withType(TYPE_CE_1)
        .build();

    // event sent in the response by the Callable service
    final var responseEvent = CloudEventBuilder.v03()
        .withId(UUID.randomUUID().toString())
        .withDataSchema(URI.create("/api/data-schema-ce-2"))
        .withSubject("subject-ce-2")
        .withSource(URI.create("/api/rossi"))
        .withData("data-ce-2".getBytes())
        .withType(TYPE_CE_2)
        .build();

    // reconcile brokers/triggers
    brokersManager.reconcile(Map.of(
        new BrokerWrapper(Broker.newBuilder()
            .setTopic(TOPIC)
            .setId(UUID.randomUUID().toString())
            .build()),
        Set.of(
            new TriggerWrapper(Trigger.newBuilder()
                .setDestination(format("http://localhost:%d%s", SERVICE_PORT, PATH_SERVICE_1))
                .putAttributes(ContextAttributes.TYPE.name().toLowerCase(), TYPE_CE_1)
                .setId(UUID.randomUUID().toString())
                .build()),
            new TriggerWrapper(Trigger.newBuilder()
                .setDestination(format("http://localhost:%d%s", SERVICE_PORT, PATH_SERVICE_2))
                .putAttributes(ContextAttributes.TYPE.name().toLowerCase(), TYPE_CE_2)
                .setId(UUID.randomUUID().toString())
                .build())
        )
    )).onFailure(context::failNow);

    // start service
    vertx.createHttpServer()
        .exceptionHandler(context::failNow)
        .requestHandler(request -> VertxMessageFactory.createReader(request)
            .map(MessageReader::toEvent)
            .onFailure(context::failNow)
            .onSuccess(event -> {

              if (request.path().equals(PATH_SERVICE_1)) { // service 1 receives requestEvent
                context.verify(() -> {
                  assertThat(event).isEqualTo(requestEvent);
                  checkpoints.flag(); // 2
                });

                VertxMessageFactory.createWriter(request.response()).writeBinary(responseEvent);
              }
              if (request.path().equals(PATH_SERVICE_2)) { // service 2 receives responseEvent
                context.verify(() -> {
                  assertThat(event).isEqualTo(responseEvent);
                  checkpoints.flag(); // 3
                });
              }
            }))
        .listen(SERVICE_PORT, "localhost");

    // do the request to the Broker receiver
    final var request = vertx.createHttpClient()
        .post(INGRESS_PORT, "localhost", format("%s/%s", BROKER_NAMESPACE, BROKER_NAME))
        .exceptionHandler(context::failNow)
        .handler(response -> context.verify(() -> {
          assertThat(response.statusCode()).isLessThan(300); // verify it's a 2xx response
          checkpoints.flag(); // 1
        }));

    VertxMessageFactory.createWriter(request).writeBinary(requestEvent);
  }

  @AfterAll
  public static void teardown(final VertxTestContext context) {

    // TODO figure out why shutdown times out Vertx context even with timeout increased.
    // kafkaCluster.shutdown();

    if (!dataDir.delete()) {
      dataDir.deleteOnExit();
    }

    context.completeNow();
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

  private static void setUpDispatcher(final Vertx vertx) {

    final ConsumerRecordOffsetStrategyFactory<String, CloudEvent>
        consumerRecordOffsetStrategyFactory = ConsumerRecordOffsetStrategyFactory.create();

    final var consumerConfigs = new Properties();
    consumerConfigs.put(BOOTSTRAP_SERVERS_CONFIG, format("localhost:%d", KAFKA_PORT));
    consumerConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class);

    final var producerConfigs = producerConfigs();

    final var consumerVerticleFactory = new HttpConsumerVerticleFactory(
        consumerRecordOffsetStrategyFactory,
        consumerConfigs,
        vertx.createHttpClient(),
        vertx,
        producerConfigs
    );

    brokersManager = new BrokersManager<>(
        vertx,
        consumerVerticleFactory,
        10,
        10
    );
  }

  private static void setUpReceiver(final Vertx vertx, final VertxTestContext context) {

    final var checkpoint = context.checkpoint(1);

    final Properties configs = producerConfigs();
    final KafkaProducer<String, CloudEvent> producer = KafkaProducer.create(vertx, configs);

    final var handler = new RequestHandler<>(producer, new CloudEventRequestToRecordMapper());

    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(INGRESS_PORT);

    final var verticle = new HttpVerticle(httpServerOptions, handler);

    vertx.deployVerticle(verticle, context.succeeding(h -> checkpoint.flag()));
  }

  private static Properties producerConfigs() {
    final var configs = new Properties();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, format("localhost:%d", KAFKA_PORT));
    configs.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
    return configs;
  }
}
