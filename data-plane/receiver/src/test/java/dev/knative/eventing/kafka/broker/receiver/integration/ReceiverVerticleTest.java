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

package dev.knative.eventing.kafka.broker.receiver.integration;

import static org.assertj.core.api.Assertions.assertThat;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.TextNode;
import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.BrokerWrapper;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig;
import dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper;
import dev.knative.eventing.kafka.broker.receiver.HttpVerticle;
import dev.knative.eventing.kafka.broker.receiver.RequestHandler;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(VertxExtension.class)
public class ReceiverVerticleTest {

  private static final int TIMEOUT = 10;
  private static final int PORT = 8082;

  private static WebClient webClient;

  private static MockProducer<String, CloudEvent> mockProducer;
  private static RequestHandler<String, CloudEvent> handler;

  @BeforeAll
  public static void setUp(final Vertx vertx, final VertxTestContext testContext) {
    webClient = WebClient.create(vertx);
    ReceiverVerticleTest.mockProducer = new MockProducer<>(
        true,
        new StringSerializer(),
        new CloudEventSerializer()
    );
    KafkaProducer<String, CloudEvent> producer = KafkaProducer.create(vertx, mockProducer);
    handler = new RequestHandler<>(
        new Properties(),
        new CloudEventRequestToRecordMapper(),
        properties -> producer
    );

    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    httpServerOptions.setHost("localhost");
    final var verticle = new HttpVerticle(httpServerOptions, handler);
    vertx.deployVerticle(verticle, testContext.succeeding(ar -> testContext.completeNow()));
  }

  @BeforeEach
  public void cleanUp() {
    mockProducer.clear();
  }

  @AfterAll
  public static void tearDown() {
    webClient.close();
  }

  @ParameterizedTest
  @MethodSource({"getValidNonValidEvents"})
  public void shouldProduceMessagesReceivedConcurrently(
      final TestCase tc,
      final VertxTestContext context) throws InterruptedException {

    final var checkpoints = context.checkpoint(1);
    final var countDown = new CountDownLatch(1);

    final var wait = new CountDownLatch(1);

    handler.reconcile(Map.of(tc.broker, new HashSet<>()))
        .onFailure(context::failNow)
        .onSuccess(v -> wait.countDown());

    wait.await(TIMEOUT, TimeUnit.SECONDS);

    tc.requestSender.apply(webClient.post(PORT, "localhost", tc.path))
        .onFailure(context::failNow)
        .onSuccess(response -> {
          assertThat(response.statusCode())
              .as("verify path: " + tc.path)
              .isEqualTo(tc.responseStatusCode);

          checkpoints.flag();
          countDown.countDown();
        });

    countDown.await(TIMEOUT, TimeUnit.SECONDS);

    if (mockProducer.history().size() > 0) {
      assertThat(mockProducer.history()).containsExactlyInAnyOrder(tc.record);
    }
  }

  private static List<TestCase> getValidNonValidEvents() {
    return Arrays.asList(
        new TestCase(
            null,
            "/broker-ns/broker-name1",
            ceRequestSender(new CloudEventBuilder()
                .withSubject("subject")
                .withSource(URI.create("/hello"))
                .withType("type")
                .withId("1234")
                .build()),
            RequestHandler.BROKER_NOT_FOUND,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setPath("/broker-ns/broker-name")
                    .build()
            )),
        new TestCase(
            null,
            "/broker-ns/broker-name/hello",
            ceRequestSender(new CloudEventBuilder()
                .withSubject("subject")
                .withSource(URI.create("/hello"))
                .withType("type")
                .withId("1234")
                .build()),
            RequestHandler.BROKER_NOT_FOUND,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setPath("/broker-name/hello")
                    .build()
            )),
        new TestCase(
            null,
            "/broker-ns/h/hello",
            ceRequestSender(new CloudEventBuilder()
                .withSubject("subject")
                .withSource(URI.create("/hello"))
                .withType("type")
                .withId("1234")
                .build()),
            RequestHandler.BROKER_NOT_FOUND,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setPath("/h/hello")
                    .build()
            )),
        new TestCase(
            new ProducerRecord<>(
                "topic-name-1",
                null,
                new io.cloudevents.core.v03.CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()
            ),
            "/broker-ns/broker-name1",
            ceRequestSender(new io.cloudevents.core.v03.CloudEventBuilder()
                .withSubject("subject")
                .withSource(URI.create("/hello"))
                .withType("type")
                .withId("1234")
                .build()),
            RequestHandler.RECORD_PRODUCED,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setTopic("topic-name-1")
                    .setPath("/broker-ns/broker-name1")
                    .build()
            )),
        new TestCase(
            null,
            "/broker-ns/broker-name",
            request -> request.sendBuffer(Buffer.buffer("this is not a cloud event")),
            RequestHandler.MAPPER_FAILED,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setPath("/broker-ns/broker-name")
                    .build()
            )),
        new TestCase(
            null,
            "/broker-ns/broker-name/hello",
            request -> request.sendBuffer(Buffer.buffer("this is not a cloud event")),
            RequestHandler.BROKER_NOT_FOUND,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setPath("/broker-ns/broker-name")
                    .build()
            )),
        new TestCase(
            null,
            "/broker-ns/broker-name3",
            request -> {
              final var objectMapper = new ObjectMapper();
              final var objectNode = objectMapper.createObjectNode();
              objectNode.set("hello", new FloatNode(1.24f));
              objectNode.set("data", objectMapper.createObjectNode());
              request.headers().set("Content-Type", "application/json");
              return request.sendBuffer(Buffer.buffer(objectNode.toString()));
            },
            RequestHandler.MAPPER_FAILED,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setPath("/broker-ns/broker-name3")
                    .build()
            )),
        new TestCase(
            null,
            "/broker-ns/broker-name4",
            request -> {
              final var objectMapper = new ObjectMapper();
              final var objectNode = objectMapper.createObjectNode();
              objectNode.set("specversion", new TextNode("1.0"));
              objectNode.set("type", new TextNode("my-type"));
              objectNode.set("source", new TextNode("my-source"));
              objectNode.set("data", objectMapper.createObjectNode());
              request.headers().set("Content-Type", "application/json");
              return request.sendBuffer(Buffer.buffer(objectNode.toString()));
            },
            RequestHandler.MAPPER_FAILED,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setPath("/broker-ns/broker-name4")
                    .build()
            )),
        new TestCase(
            new ProducerRecord<>(
                "topic-name-42",
                null,
                new io.cloudevents.core.v1.CloudEventBuilder()
                    .withSubject("subject")
                    .withSource(URI.create("/hello"))
                    .withType("type")
                    .withId("1234")
                    .build()
            ),
            "/broker-ns/broker-name5",
            ceRequestSender(new io.cloudevents.core.v1.CloudEventBuilder()
                .withSubject("subject")
                .withSource(URI.create("/hello"))
                .withType("type")
                .withId("1234")
                .build()),
            RequestHandler.RECORD_PRODUCED,
            new BrokerWrapper(
                BrokersConfig.Broker.newBuilder()
                    .setTopic("topic-name-42")
                    .setPath("/broker-ns/broker-name5")
                    .build()
            ))
    );
  }

  private static Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> ceRequestSender(
      final CloudEvent event) {
    return request -> VertxMessageFactory.createWriter(request).writeBinary(event);
  }

  static final class TestCase {

    final ProducerRecord<String, CloudEvent> record;
    final String path;
    final Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> requestSender;
    final int responseStatusCode;
    final Broker broker;

    TestCase(
        final ProducerRecord<String, CloudEvent> record,
        final String path,
        final Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> requestSender,
        final int responseStatusCode, Broker broker) {

      this.path = path;
      this.requestSender = requestSender;
      this.responseStatusCode = responseStatusCode;
      this.record = record;
      this.broker = broker;
    }
  }
}
