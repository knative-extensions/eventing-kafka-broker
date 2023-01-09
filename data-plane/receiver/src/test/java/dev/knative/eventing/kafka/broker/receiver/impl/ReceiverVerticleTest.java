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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.TextNode;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractMessageCodec;
import dev.knative.eventing.kafka.broker.core.eventbus.ContractPublisher;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.impl.ResourcesReconcilerMessageHandler;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.testing.CloudEventSerializerMock;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.ControlPlaneProbeRequestUtil;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.IngressRequestHandlerImpl;
import dev.knative.eventing.kafka.broker.receiver.main.ReceiverEnv;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ReceiverVerticleTest {

  private static final int TIMEOUT = 10;
  private static final int PORT = 8082;

  private static WebClient webClient;

  private static MockProducer<String, CloudEvent> mockProducer;
  private static IngressProducerReconcilableStore store;

  static {
    BackendRegistries.setupBackend(new MicrometerMetricsOptions()
      .setMicrometerRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT))
      .setRegistryName(Metrics.METRICS_REGISTRY_NAME));
  }

  private PrometheusMeterRegistry registry;

  @BeforeEach
  public void setUp(final Vertx vertx, final VertxTestContext testContext) {
    ContractMessageCodec.register(vertx.eventBus());

    webClient = WebClient.create(vertx);
    ReceiverVerticleTest.mockProducer = new MockProducer<>(
      true,
      new StringSerializer(),
      new CloudEventSerializerMock()
    );
    KafkaProducer<String, CloudEvent> producer = KafkaProducer.create(vertx, mockProducer);

    store = new IngressProducerReconcilableStore(
      AuthProvider.noAuth(),
      new Properties(),
      properties -> producer
    );

    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    httpServerOptions.setHost("localhost");
    final var env = mock(ReceiverEnv.class);
    when(env.getLivenessProbePath()).thenReturn("/healthz");
    when(env.getReadinessProbePath()).thenReturn("/readyz");
    final var verticle = new ReceiverVerticle(
      env,
      httpServerOptions,
      v -> store,
      new IngressRequestHandlerImpl(
        StrictRequestToRecordMapper.getInstance(),
        registry
      )
    );
    vertx.deployVerticle(verticle, testContext.succeeding(ar -> testContext.completeNow()));
  }

  @BeforeEach
  public void cleanUp() {
    mockProducer.clear();
  }

  @AfterEach
  public void tearDownEach() {
    registry.close();
  }

  @AfterAll
  public static void tearDown() {
    webClient.close();
  }

  @ParameterizedTest
  @MethodSource({"getValidNonValidEvents"})
  public void shouldProduceMessagesReceivedConcurrently(
    final TestCase tc,
    final Vertx vertx,
    final VertxTestContext context) throws InterruptedException {

    final var checkpoints = context.checkpoint(1);
    final var countDown = new CountDownLatch(1);

    new ContractPublisher(vertx.eventBus(), ResourcesReconcilerMessageHandler.ADDRESS)
      .accept(tc.getContract());

    tc.requestSender.apply(webClient.post(PORT, "localhost", tc.path))
      .onFailure(context::failNow)
      .onSuccess(response -> vertx.setTimer(1000, ignored -> context.verify(() -> {
        assertThat(response.statusCode())
          .as("verify path: " + tc.path)
          .isEqualTo(tc.responseStatusCode);

        final var expectedCount = (double) tc.badRequestCount + (double) tc.produceEventCount;

        final var defaultCounter = Counter.builder(Metrics.EVENTS_COUNT).register(Metrics.getRegistry());
        Counter counter = defaultCounter;
        try {
          if (expectedCount > 0) {
            counter = registry.get(Metrics.EVENTS_COUNT).counters()
              .stream()
              .reduce((a, b) -> b) // get last element
              .orElse(defaultCounter);
          }
        } catch (MeterNotFoundException ignored1) {
        }

        assertThat(counter.count())
          .describedAs("Counter: " + StreamSupport
            .stream(counter.measure().spliterator(), false)
            .map(Measurement::toString)
            .collect(Collectors.joining()))
          .isEqualTo(expectedCount);

        if (tc.expectedDispatchLatency) {
          final var eventDispatchLatency = registry.get(Metrics.EVENT_DISPATCH_LATENCY)
            .summaries()
            .stream()
            .reduce((a, b) -> b)
            .get();

          assertThat(eventDispatchLatency.max()).isGreaterThan(0);
          assertThat(eventDispatchLatency.totalAmount()).isGreaterThan(0);
          assertThat(eventDispatchLatency.mean()).isGreaterThan(0);
        }

        checkpoints.flag();
        countDown.countDown();
      })));

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
        NOT_FOUND.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name"))
          .build()
      ),
      new TestCase(
        null,
        "/broker-ns/broker-name/hello",
        ceRequestSender(new CloudEventBuilder()
          .withSubject("subject")
          .withSource(URI.create("/hello"))
          .withType("type")
          .withId("1234")
          .build()),
        NOT_FOUND.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-name/hello"))
          .build()
      ),
      new TestCase(
        null,
        "/broker-ns/h/hello",
        ceRequestSender(new CloudEventBuilder()
          .withSubject("subject")
          .withSource(URI.create("/hello"))
          .withType("type")
          .withId("1234")
          .build()),
        NOT_FOUND.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/h/hello"))
          .build()
      ),
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
        202,
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic-name-1")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name1"))
          .build()
      ),
      new TestCase(
        null,
        "/broker-ns/broker-name",
        request -> request.sendBuffer(Buffer.buffer("this is not a cloud event")),
        BAD_REQUEST.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name"))
          .build()
      ),
      new TestCase(
        null,
        "/broker-ns/broker-name/hello",
        request -> request.sendBuffer(Buffer.buffer("this is not a cloud event")),
        NOT_FOUND.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name"))
          .build()
      ),
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
        BAD_REQUEST.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name3"))
          .build()
      ),
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
        BAD_REQUEST.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name4"))
          .build()
      ),
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
        ACCEPTED.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic-name-42")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name5"))
          .build()
      ),
      new TestCase(
        new ProducerRecord<>(
          "topic-name-42",
          null,
          null
        ),
        "/broker-ns/broker-name5",
        probeRequestSender(null),
        OK.code(),
        DataPlaneContract.Resource.newBuilder()
          .setUid("1")
          .addTopics("topic-name-42")
          .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name5"))
          .build()
      ),
      new TestCase(
        new ProducerRecord<>(
          "topic-name-42",
          null,
          null
        ),
        "/broker-ns/broker-name5",
        probeRequestSender(null),
        NOT_FOUND.code(),
        DataPlaneContract.Resource.newBuilder().build()
      )
    );
  }

  private static Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> ceRequestSender(
    final CloudEvent event) {
    return request -> VertxMessageFactory.createWriter(request).writeBinary(event);
  }

  private static Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> probeRequestSender(
    final CloudEvent event) {
    return request -> request
      .method(HttpMethod.GET)
      .putHeader(ControlPlaneProbeRequestUtil.PROBE_HEADER_NAME, ControlPlaneProbeRequestUtil.PROBE_HEADER_VALUE)
      .send();
  }

  static final class TestCase {

    final ProducerRecord<String, CloudEvent> record;
    final String path;
    final Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> requestSender;
    final int responseStatusCode;
    final DataPlaneContract.Resource resource;
    final int badRequestCount;
    final int produceEventCount;
    final boolean expectedDispatchLatency;

    TestCase(
      final ProducerRecord<String, CloudEvent> record,
      final String path,
      final Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> requestSender,
      final int responseStatusCode,
      final DataPlaneContract.Resource resource) {

      this.path = path;
      this.requestSender = requestSender;
      this.responseStatusCode = responseStatusCode;
      this.record = record;
      this.resource = DataPlaneContract.Resource.newBuilder(resource)
        .build();

      int badRequestCount = 0;
      int produceEventsCount = 0;
      if (responseStatusCode == BAD_REQUEST.code()) {
        badRequestCount = 1;
      } else if (responseStatusCode == ACCEPTED.code()) {
        produceEventsCount = 1;
      }

      this.expectedDispatchLatency = responseStatusCode == ACCEPTED.code();

      this.badRequestCount = badRequestCount;
      this.produceEventCount = produceEventsCount;
    }

    DataPlaneContract.Contract getContract() {
      return DataPlaneContract.Contract.newBuilder()
        .addResources(this.resource)
        .build();
    }

    @Override
    public String toString() {
      return "TestCase{" +
        "path='" + path + '\'' +
        ", responseStatusCode=" + responseStatusCode +
        '}';
    }
  }
}
