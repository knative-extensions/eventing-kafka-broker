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

package dev.knative.eventing.kafka.broker.dispatcher.http;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
@ExtendWith(VertxExtension.class)
public class HttpSinkResponseHandlerTest {

  private static final String TOPIC = "t1";

  @Test
  public void shouldSucceedOnUnknownEncodingAndEmptyResponse(final Vertx vertx, final VertxTestContext context) {
    final var producer = new MockProducer<>(
      true,
      new StringSerializer(),
      new CloudEventSerializer()
    );
    final var handler = new HttpSinkResponseHandler(
      vertx,
      TOPIC,
      KafkaProducer.create(vertx, producer)
    );

    // Empty response
    final HttpResponse<Buffer> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(202);
    when(response.body()).thenReturn(Buffer.buffer());
    when(response.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());

    context
      .assertComplete(handler.handle(response))
      .onSuccess(v -> context.completeNow());
  }

  @Test
  public void shouldSucceedOnUnknownEncodingAndNullResponseBody(final Vertx vertx, final VertxTestContext context) {
    final var producer = new MockProducer<>(
      true,
      new StringSerializer(),
      new CloudEventSerializer()
    );
    final var handler = new HttpSinkResponseHandler(
      vertx,
      TOPIC,
      KafkaProducer.create(vertx, producer)
    );

    // Empty response
    final HttpResponse<Buffer> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(202);
    when(response.body()).thenReturn(null);
    when(response.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());

    context
      .assertComplete(handler.handle(response))
      .onSuccess(v -> context.completeNow());
  }


  @Test
  public void shouldFailOnUnknownEncodingAndNonEmptyResponse(final Vertx vertx, final VertxTestContext context) {

    final var producer = new MockProducer<>(
      true,
      new StringSerializer(),
      new CloudEventSerializer()
    );
    final var handler = new HttpSinkResponseHandler(
      vertx,
      TOPIC,
      KafkaProducer.create(vertx, producer)
    );

    // Empty response
    final HttpResponse<Buffer> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(202);
    when(response.body()).thenReturn(Buffer.buffer(new byte[]{'a'}));
    when(response.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());

    context
      .assertFailure(handler.handle(response))
      .onSuccess(s -> context.failNow(new Exception("expected failed future")))
      .onFailure(v -> context.completeNow());
  }

  @Test
  public void shouldSendRecord(final Vertx vertx, final VertxTestContext context)
    throws InterruptedException {
    final var producer = new MockProducer<>(
      true,
      new StringSerializer(),
      new CloudEventSerializer()
    );
    final var handler = new HttpSinkResponseHandler(
      vertx,
      TOPIC,
      KafkaProducer.create(vertx, producer)
    );

    final var event = new CloudEventBuilder()
      .withId("1234")
      .withSource(URI.create("/api"))
      .withSubject("subject")
      .withType("type")
      .build();

    final HttpResponse<Buffer> response = mock(HttpResponse.class);
    when(response.body()).thenReturn(Buffer.buffer(
      EventFormatProvider.getInstance()
        .resolveFormat("application/cloudevents+json")
        .serialize(event)
    ));
    when(response.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap()
      .set(HttpHeaders.CONTENT_TYPE, "application/cloudevents+json")
    );

    final var wait = new CountDownLatch(1);
    handler.handle(response)
      .onSuccess(ignored -> wait.countDown())
      .onFailure(context::failNow);

    wait.await();

    Assertions.assertThat(producer.history())
      .containsExactlyInAnyOrder(new ProducerRecord<>(TOPIC, null, event));
    context.completeNow();
  }
}
