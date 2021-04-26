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
package dev.knative.eventing.control.protocol.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

/**
 * This class sends requests to event bus in order, waiting every time for the previous one to reply before sending the next one.
 */
public class SerializedEventBusRequester {

  private final EventBus eventBus;
  private final String address;
  private final DeliveryOptions deliveryOptions;

  private final Queue<Map.Entry<Object, Promise<Message>>> queue;

  public SerializedEventBusRequester(EventBus eventBus, String address, DeliveryOptions deliveryOptions) {
    this.eventBus = eventBus;
    this.address = address;
    this.deliveryOptions = deliveryOptions;
    this.queue = new ArrayDeque<>();
  }

  @SuppressWarnings("unchecked")
  public <T> Future<Message<T>> request(Object object) {
    Promise prom = Promise.promise();
    boolean wasEmpty = queue.isEmpty();
    queue.offer(Map.entry(object, prom));
    if (wasEmpty) { // If no elements in the queue, then we need to start consuming it
      consume();
    }

    return (Future<Message<T>>) prom.future();
  }

  @SuppressWarnings("unchecked")
  void consume() {
    Map.Entry<Object, Promise<Message>> entry = this.queue.peek();
    if (entry == null) {
      return; // No task to process
    }
    eventBus.request(this.address, entry.getKey(), this.deliveryOptions)
      .onComplete(ar -> {
        // Let's propagate the async result to the offer call
        entry.getValue()
          .handle((AsyncResult) ar);

        // Remove the element from the queue and consume again
        queue.poll();
        consume();
      });
  }

}
