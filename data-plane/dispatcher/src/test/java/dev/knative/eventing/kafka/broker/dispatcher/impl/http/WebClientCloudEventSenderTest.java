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
package dev.knative.eventing.kafka.broker.dispatcher.impl.http;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static dev.knative.eventing.kafka.broker.dispatcher.impl.http.WebClientCloudEventSender.isRetryableStatusCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class WebClientCloudEventSenderTest {

  @Test
  public void shouldCloseCircuitBreakerAndWebClient(final VertxTestContext context) {

    final var circuitBreaker = mock(CircuitBreaker.class);
    when(circuitBreaker.close()).thenReturn(circuitBreaker);

    final var webClient = mock(WebClient.class);
    doNothing().when(webClient).close();

    final var consumerRecordSender = new WebClientCloudEventSender(
      webClient, circuitBreaker, new CircuitBreakerOptions(), "http://localhost:12345"
    );

    consumerRecordSender.close()
      .onFailure(context::failNow)
      .onSuccess(r -> context.verify(() -> {
        verify(circuitBreaker, times(1)).close();
        verify(webClient, times(1)).close();
        context.completeNow();
      }));
  }

  @ParameterizedTest
  @MethodSource("retryableStatusCodes")
  public void shouldRetryRetryableStatusCodes(final Integer statusCode) {
    assertThat(isRetryableStatusCode(statusCode)).isTrue();
  }

  @ParameterizedTest
  @MethodSource("nonRetryableStatusCodes")
  public void shouldNotRetryNonRetryableStatusCodes(final Integer statusCode) {
    assertThat(isRetryableStatusCode(statusCode)).isFalse();
  }

  public static Stream<Integer> nonRetryableStatusCodes() {
    return Stream.concat(
      Stream.concat(
        IntStream.range(200, 404).boxed(),
        IntStream.range(405, 408).boxed()
      ),
      Stream.concat(
        IntStream.range(410, 429).boxed(),
        IntStream.range(430, 500).boxed()
      )
    );
  }

  public static Stream<Integer> retryableStatusCodes() {
    return Stream.concat(
      IntStream.range(500, 600).boxed(),
      IntStream.of(404, 408, 409, 429).boxed()
    );
  }
}
