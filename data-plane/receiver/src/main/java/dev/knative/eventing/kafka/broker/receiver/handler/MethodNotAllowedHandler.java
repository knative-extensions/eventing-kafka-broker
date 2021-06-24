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

import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;

/**
 * Handler checking that the provided method is allowed.
 * <p>
 * This class is stateless, hence thread safe and shareable among verticles.
 */
public class MethodNotAllowedHandler implements Handler<HttpServerRequest> {

  private static final Logger logger = LoggerFactory.getLogger(MethodNotAllowedHandler.class);

  private static class SingletonContainer {
    private static final MethodNotAllowedHandler INSTANCE = new MethodNotAllowedHandler();
  }

  public static MethodNotAllowedHandler getInstance() {
    return MethodNotAllowedHandler.SingletonContainer.INSTANCE;
  }

  @Override
  public void handle(final HttpServerRequest request) {
    if (request.method() != HttpMethod.POST) {
      request.response().setStatusCode(METHOD_NOT_ALLOWED.code()).end();

      logger.warn("Only POST method is allowed. Method not allowed: {}",
        keyValue("method", request.method())
      );
    }
  }
}
