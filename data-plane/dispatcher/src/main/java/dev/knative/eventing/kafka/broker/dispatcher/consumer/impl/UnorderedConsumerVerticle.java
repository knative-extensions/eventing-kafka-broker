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

import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link io.vertx.core.Verticle} implements an unordered consumer logic, as described in {@link dev.knative.eventing.kafka.broker.dispatcher.consumer.ConsumerType#UNORDERED}.
 */
public final class UnorderedConsumerVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(UnorderedConsumerVerticle.class);

  private final Set<String> topics;
  private final Function<Vertx,> initializer;

  private KafkaConsumer<String, CloudEvent> consumer;
  private RecordDispatcher recordDispatcher;
  private Supplier<Future<?>> closer;

  public UnorderedConsumerVerticle(KafkaConsumer<String, CloudEvent> consumer, RecordDispatcher recordDispatcher,
                                   final Set<String> topics) {
    Objects.requireNonNull(consumer);
    Objects.requireNonNull(recordDispatcher);
    Objects.requireNonNull(topics);

    this.consumer = consumer;
    this.recordDispatcher = recordDispatcher;
    this.topics = topics;
  }

  @Override
  public void start(Promise<Void> startPromise) {

    this.consumer.exceptionHandler(this::exceptionHandler);
    this.consumer.handler(this.recordDispatcher);
    this.consumer.subscribe(this.topics, startPromise);
  }

  private void exceptionHandler(Throwable cause) {
    // TODO Add context (consumer group, resource id, etc)
    // TODO Send message on event bus
    logger.error("Consumer exception", cause);
    this.context.exceptionHandler().handle(cause); // TODO why that?
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    logger.info("Stopping consumer");

    CompositeFuture.all(
      this.consumer.close(),
      this.recordDispatcher.close()
    ).<Void>mapEmpty().onComplete(stopPromise);
  }
}
