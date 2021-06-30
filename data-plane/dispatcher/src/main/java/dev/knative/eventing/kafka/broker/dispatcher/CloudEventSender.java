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
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;

/**
 * This interface describes a component that sends {@link CloudEvent} to some http service and then returns back a response.
 */
public interface CloudEventSender extends AsyncCloseable {

  /**
   * Send the given event. (the event passed the filter)
   *
   * @param event event to send
   * @return a successful future or a failed future.
   */
  Future<HttpResponse<Buffer>> send(CloudEvent event);

  /**
   * Create a noop {@link CloudEventSender} that fails every send with the specified message.
   *
   * @param failureMessage future failure message when invoking send.
   * @return An implementation of this interface that always fails the send invocations with the specified message and always succeeds the close invocations.
   */
  static CloudEventSender noop(String failureMessage) {
    return new CloudEventSender() {
      @Override
      public Future<HttpResponse<Buffer>> send(CloudEvent event) {
        return Future.failedFuture(failureMessage);
      }

      @Override
      public Future<Void> close() {
        return Future.succeededFuture();
      }
    };
  }
}
