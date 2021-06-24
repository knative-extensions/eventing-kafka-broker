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
package dev.knative.eventing.kafka.broker.receiver;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.testing.CloudEventSerializerMock;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class ReceiverTracingVerticleTest {

  private static final int TIMEOUT = 10;
  private static final int PORT = 8083;

  private Vertx vertx;
  private InMemorySpanExporter spanExporter;
  private WebClient webClient;
  private MockProducer<String, CloudEvent> mockProducer;
  private RequestMapper handler;

  static {
    BackendRegistries.setupBackend(new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME));
  }

  @BeforeEach
  public void setup() throws ExecutionException, InterruptedException {
    this.spanExporter = InMemorySpanExporter.create();
    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
      .addSpanProcessor(SimpleSpanProcessor.create(this.spanExporter))
      .addSpanProcessor(SimpleSpanProcessor.create(new LoggingSpanExporter()))
      // Uncomment this line if you want to try locally
      //.addSpanProcessor(SimpleSpanProcessor.create(ZipkinSpanExporter.builder().build()))
      .setSampler(Sampler.alwaysOn())
      .build();
    OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .setTracerProvider(tracerProvider)
      .build();

    this.vertx = Vertx.vertx(new VertxOptions().setTracingOptions(new OpenTelemetryOptions(openTelemetry)));

    this.webClient = WebClient.create(vertx,
      (WebClientOptions) new WebClientOptions().setTracingPolicy(TracingPolicy.ALWAYS)
    );
    this.mockProducer = new MockProducer<>(
      true,
      new StringSerializer(),
      new CloudEventSerializerMock()
    );

    this.handler = new RequestMapper(
      null,
      new Properties(),
      StrictRequestToRecordMapper.getInstance(),
      properties -> KafkaProducer.create(vertx, mockProducer),
      new CumulativeCounter(mock(Id.class)),
      new CumulativeCounter(mock(Id.class))
    );

    final var verticle = new ReceiverVerticle(
      new HttpServerOptions()
        .setPort(PORT)
        .setHost("localhost")
        .setTracingPolicy(TracingPolicy.PROPAGATE),
      v -> handler
    );
    vertx.deployVerticle(verticle)
      .toCompletionStage()
      .toCompletableFuture()
      .get();
  }

  @AfterEach
  public void tearDown() throws ExecutionException, InterruptedException {
    vertx
      .close()
      .toCompletionStage()
      .toCompletableFuture()
      .get();
  }

  @Test
  public void traceIsPropagated() throws ExecutionException, InterruptedException, TimeoutException {
    CloudEvent inputEvent = new CloudEventBuilder()
      .withSubject("subject")
      .withSource(URI.create("/hello"))
      .withType("type")
      .withId("1234")
      .build();

    DataPlaneContract.Resource contract = DataPlaneContract.Resource.newBuilder()
      .setUid("1")
      .addTopics("topic-name-42")
      .setIngress(DataPlaneContract.Ingress.newBuilder().setPath("/broker-ns/broker-name"))
      .build();

    String path = "/broker-ns/broker-name";

    this.handler.onNewIngress(contract, contract.getIngress())
      .toCompletionStage()
      .toCompletableFuture()
      .get();

    HttpResponse<Buffer> response = vertx.<HttpResponse<Buffer>>executeBlocking(promise -> {
      VertxMessageFactory
        .createWriter(webClient.post(PORT, "localhost", path))
        .writeBinary(inputEvent)
        .onComplete(promise);
    }).toCompletionStage()
      .toCompletableFuture()
      .get(TIMEOUT, TimeUnit.SECONDS);

    assertThat(response.statusCode())
      .isEqualTo(RequestMapper.RECORD_PRODUCED);

    if (mockProducer.history().size() > 0) {
      assertThat(mockProducer.history())
        .extracting(ProducerRecord::value)
        .containsExactlyInAnyOrder(inputEvent);

      assertThat(mockProducer.history())
        .extracting(ProducerRecord::headers)
        .extracting(h -> h.lastHeader("traceparent"))
        .isNotNull()
        .isNotEmpty();
    }

    assertThat(spanExporter.getFinishedSpanItems())
      .hasSize(3);
  }
}
