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
package dev.knative.eventing.kafka.broker.dispatcher.consumer.impl;

import dev.knative.eventing.kafka.broker.dispatcher.consumer.DeliveryOrder;
import io.vertx.core.Promise;
import java.util.Set;

/**
 * This {@link io.vertx.core.Verticle} implements an unordered consumer logic, as described in {@link DeliveryOrder#UNORDERED}.
 */
public final class UnorderedConsumerVerticle extends BaseConsumerVerticle {

  public UnorderedConsumerVerticle(Initializer initializer,
                                   Set<String> topics) {
    super(initializer, topics);
  }

  @Override
  void startConsumer(Promise<Void> startPromise) {
    this.consumer.exceptionHandler(this::exceptionHandler);
    this.consumer.handler(record -> this.recordDispatcher.dispatch(record));
    this.consumer.subscribe(this.topics, startPromise);
  }

}
