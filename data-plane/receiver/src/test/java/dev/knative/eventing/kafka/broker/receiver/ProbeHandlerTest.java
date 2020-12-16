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
package dev.knative.eventing.kafka.broker.receiver;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
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

  private WebClient webClient;
  private HttpServer server;

  @BeforeEach
  public void setUp(final Vertx vertx, final VertxTestContext context) {
    final var httpServerOptions = new HttpServerOptions();
    httpServerOptions.setPort(PORT);
    httpServerOptions.setHost("localhost");

    this.webClient = WebClient.create(vertx);

    this.server = vertx.createHttpServer(httpServerOptions);
    this.server
        .requestHandler(
            new SimpleProbeHandlerDecorator(
                LIVENESS_PATH,
                READINESS_PATH,
                r -> r.response().setStatusCode(NEXT_HANDLER_STATUS_CODE).end()))
        .listen(httpServerOptions.getPort(), httpServerOptions.getHost())
        .onComplete(context.succeedingThenComplete());
  }

  @AfterEach
  public void tearDown(final VertxTestContext context) {
    this.webClient.close();
    this.server.close().onComplete(context.succeedingThenComplete());
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

  private void mustReceiveStatusCodeOnPath(
      final VertxTestContext context, final int expectedStatusCode, final String path) {
    webClient
        .get(PORT, "localhost", path)
        .send()
        .onSuccess(
            response ->
                context.verify(
                    () -> {
                      assertThat(response.statusCode()).isEqualTo(expectedStatusCode);
                      context.completeNow();
                    }))
        .onFailure(context::failNow);
  }
}
