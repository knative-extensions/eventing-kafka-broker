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

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.util.function.Function;
import java.util.function.Supplier;

public class SinkResponseHandlerMock implements SinkResponseHandler {

  private final Supplier<Future<?>> onClose;
  private final Function<HttpResponse<Buffer>, Future<Void>> onSend;

  public SinkResponseHandlerMock(final Supplier<Future<?>> onClose,
                                 final Function<HttpResponse<Buffer>, Future<Void>> onSend) {
    this.onClose = onClose;
    this.onSend = onSend;
  }

  @Override
  public Future<Void> handle(HttpResponse<Buffer> response) {
    return this.onSend.apply(response);
  }

  @Override
  public Future<?> close() {
    return this.onClose.get();
  }
}
