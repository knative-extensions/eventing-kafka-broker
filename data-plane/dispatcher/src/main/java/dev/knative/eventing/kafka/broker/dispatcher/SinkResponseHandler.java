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

/**
 * SinkResponseHandler is responsible for reading the response and acting on it based on its content.
 *
 * @param <R> Response type.
 */
public interface SinkResponseHandler<R> {

  /**
   * Handler the response.
   *
   * @param response Response to handle.
   * @return A succeeded or failed future.
   */
  Future<Void> handle(final R response);

  /**
   * Close resources.
   *
   * @return A succeeded or failed future.
   */
  Future<?> close();
}
