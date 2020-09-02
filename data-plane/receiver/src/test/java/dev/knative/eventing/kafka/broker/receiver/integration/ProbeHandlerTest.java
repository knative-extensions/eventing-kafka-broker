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


import dev.knative.eventing.kafka.broker.receiver.HttpVerticle;
import dev.knative.eventing.kafka.broker.receiver.SimpleProbeHandlerDecorator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ProbeHandlerTest {

  private static final int PORT = 43999;

  private static final String LIVENESS_PATH = "/healthz";
  private static final String READINESS_PATH = "/readyz";
  private static final int OK = HttpResponseStatus.OK.code();
  private static final int NEXT_HANDLER_STATUS_CODE = HttpResponseStatus.SERVICE_UNAVAILABLE.code();

  private static WebClient webClient;

  @BeforeEach
  public void setUp(final Vertx vertx, final VertxTestContext context) {
    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    httpServerOptions.setHost("localhost");
    final var verticle = new HttpVerticle(httpServerOptions, new SimpleProbeHandlerDecorator(
        LIVENESS_PATH,
        READINESS_PATH,
        r -> r.response().setStatusCode(NEXT_HANDLER_STATUS_CODE).end()
    ));
    webClient = WebClient.create(vertx);
    vertx.deployVerticle(verticle, context.succeeding(ar -> context.completeNow()));
  }

  @AfterAll
  public static void tearDown() {
    webClient.close();
  }

  @Test
  public void testReadinessCheck(final VertxTestContext context) {
    mustReceiveStatusCodeOnPath(context, OK, READINESS_PATH);
  }

  @Test
  public void testLivenessCheck(final VertxTestContext context) {
    mustReceiveStatusCodeOnPath(context, OK, LIVENESS_PATH);
  }

  @Test
  public void shouldForwardToNextHandler(final VertxTestContext context) {
    mustReceiveStatusCodeOnPath(context, NEXT_HANDLER_STATUS_CODE, "/does-not-exists-42");
  }

  private static void mustReceiveStatusCodeOnPath(
      final VertxTestContext context,
      final int expectedStatusCode,
      final String path) {
    webClient.get(PORT, "localhost", path)
        .send()
        .onSuccess(response -> context.verify(() -> {
          assertThat(response.statusCode())
              .isEqualTo(expectedStatusCode);
          context.completeNow();
        }))
        .onFailure(context::failNow);
  }
}
