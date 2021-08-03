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
package dev.knative.eventing.kafka.broker.receiver.impl.handler;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.mockito.Mockito.mock;

public class MethodNotAllowedHandlerTest extends PreHandlerTest {

  @Test
  public void testBadMethod(final VertxTestContext context) {
    mustReceiveStatusCodeOnPath(context, METHOD_NOT_ALLOWED.code(), HttpMethod.GET, "/");
  }

  @Test
  public void testCorrectMethod(final VertxTestContext context) {
    mustReceiveStatusCodeOnPath(context, NEXT_HANDLER_STATUS_CODE, HttpMethod.POST, "/");
  }

  @Override
  @SuppressWarnings("unchecked")
  public Handler<HttpServerRequest> createHandler() {
    return new MethodNotAllowedHandler(mock(Handler.class));
  }
}
