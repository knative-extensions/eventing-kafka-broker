/*
 * Copyright © 2022 Knative Authors (knative-dev@googlegroups.com)
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
package dev.knative.eventing.kafka.broker.dispatcher.impl;

import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.*;

@Execution(ExecutionMode.CONCURRENT)
@ExtendWith(VertxExtension.class)
public class ResponseToHttpEndpointHandlerTest {

  private CloudEventSender cloudEventSender;
  private ResponseToHttpEndpointHandler handler;

  @BeforeEach
  void setUp() {
    cloudEventSender = mock(CloudEventSender.class);
    handler = new ResponseToHttpEndpointHandler(cloudEventSender);
  }

  @Test
  public void shouldSucceedOnUnknownEncodingAndEmptyResponse(final Vertx vertx, final VertxTestContext context) {
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
  public void shouldSendRecord(final Vertx vertx, final VertxTestContext context) throws InterruptedException {

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
    when(cloudEventSender.send(event)).thenReturn(Future.succeededFuture());

    final var wait = new CountDownLatch(1);
    handler.handle(response)
      .onSuccess(ignored -> wait.countDown())
      .onFailure(context::failNow);

    wait.await();

    Mockito.verify(cloudEventSender).send(event);

    context.completeNow();
  }

  @Test
  public void shouldCloseProducer(final Vertx vertx, final VertxTestContext context) {
    when(cloudEventSender.close()).thenReturn(Future.succeededFuture());

    final var responseHandler = new ResponseToHttpEndpointHandler(cloudEventSender);

    vertx.runOnContext(v -> {
      responseHandler.close()
        .onFailure(context::failNow)
        .onSuccess(r -> context.verify(() -> {
          verify(cloudEventSender, times(1)).close();
          context.completeNow();
        }));
    });
  }

}
