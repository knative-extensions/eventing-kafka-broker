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
package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;

/**
 * This interface describes a component that handles http responses.
 */
public interface ResponseHandler extends AsyncCloseable {

  /**
   * Handle the response.
   *
   * @param response Response to handle.
   * @return A succeeded or failed future.
   */
  Future<Void> handle(final HttpResponse<Buffer> response);

  /**
   * @return a noop response handler.
   */
  static ResponseHandler noop() {
    return new ResponseHandler() {
      @Override
      public Future<Void> handle(HttpResponse<Buffer> response) {
        return Future.succeededFuture();
      }

      @Override
      public Future<Void> close() {
        return Future.succeededFuture();
      }
    };
  }
}
