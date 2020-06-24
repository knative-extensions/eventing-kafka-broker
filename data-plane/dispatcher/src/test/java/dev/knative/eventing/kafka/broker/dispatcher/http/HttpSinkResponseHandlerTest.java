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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventSerializer;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.impl.HeadersAdaptor;
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
  public void shouldAlwaysSucceed(final Vertx vertx, final VertxTestContext context) {
    final var producer = new MockProducer<>(
        true,
        new StringSerializer(),
        new CloudEventSerializer()
    );
    final var handler = new HttpSinkResponseHandler(
        TOPIC,
        KafkaProducer.create(vertx, producer)
    );

    final var response = mock(HttpClientResponse.class);
    when(response.bodyHandler(any())).thenAnswer(invocation -> {
      final Handler<Buffer> handlerBuf = invocation.getArgument(0);
      handlerBuf.handle(Buffer.buffer());
      return response;
    });
    when(response.headers()).thenReturn(new HeadersAdaptor(new DefaultHttpHeaders(false)));

    final var future = handler.handle(response);

    future
        .onSuccess(ignored -> context.completeNow())
        .onFailure(context::failNow);
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
        TOPIC,
        KafkaProducer.create(vertx, producer)
    );

    final var event = new CloudEventBuilder()
        .withId("1234")
        .withSource(URI.create("/api"))
        .withSubject("subject")
        .withType("type")
        .build();

    final var response = mock(HttpClientResponse.class);
    when(response.bodyHandler(any())).thenAnswer(invocation -> {
      final Handler<Buffer> handlerBuf = invocation.getArgument(0);

      final var payload = EventFormatProvider.getInstance()
          .resolveFormat("application/cloudevents+json")
          .serialize(event);

      handlerBuf.handle(Buffer.buffer(payload));
      return response;
    });
    final var headers = new DefaultHttpHeaders(false);
    headers.add(HttpHeaders.CONTENT_TYPE, "application/cloudevents+json");
    when(response.headers()).thenReturn(new HeadersAdaptor(headers));

    final var future = handler.handle(response);

    final var wait = new CountDownLatch(1);
    future
        .onSuccess(ignored -> wait.countDown())
        .onFailure(context::failNow);

    wait.await();

    Assertions.assertThat(producer.history())
        .containsExactlyInAnyOrder(new ProducerRecord<>(TOPIC, "", event));

    context.completeNow();
  }
}