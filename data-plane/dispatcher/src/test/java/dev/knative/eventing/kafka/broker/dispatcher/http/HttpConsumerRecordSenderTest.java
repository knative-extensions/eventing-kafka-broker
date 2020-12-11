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
package dev.knative.eventing.kafka.broker.dispatcher.http;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class HttpConsumerRecordSenderTest {

  @Test
  public void shouldCloseCircuitBreakerAndWebClient(final Vertx vertx, final VertxTestContext context) {

    final var circuitBreaker = mock(CircuitBreaker.class);
    when(circuitBreaker.close()).thenReturn(circuitBreaker);

    final var webClient = mock(WebClient.class);
    doNothing().when(webClient).close();

    final var consumerRecordSender = new HttpConsumerRecordSender(
      vertx,
      "http://localhost:12345",
      circuitBreaker,
      webClient
    );

    consumerRecordSender.close()
      .onFailure(context::failNow)
      .onSuccess(r -> context.verify(() -> {
        verify(circuitBreaker, times(1)).close();
        verify(webClient, times(1)).close();
        context.completeNow();
      }));
  }
}
