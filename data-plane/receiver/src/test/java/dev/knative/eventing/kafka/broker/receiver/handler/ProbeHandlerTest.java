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
package dev.knative.eventing.kafka.broker.receiver.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

public class ProbeHandlerTest extends PreHandlerTest {

  private static final String LIVENESS_PATH = "/healthz";
  private static final String READINESS_PATH = "/readyz";
  private static final int OK = HttpResponseStatus.OK.code();

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

  @Override
  public Handler<HttpServerRequest> createHandler() {
    return new ProbeHandler(LIVENESS_PATH, READINESS_PATH);
  }
}
