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
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import dev.knative.eventing.kafka.broker.core.AsyncCloseable;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public abstract class BaseConsumerVerticle extends AbstractVerticle {

  public interface Initializer extends BiFunction<Vertx, BaseConsumerVerticle, Future<Void>> {
  }

  private static final Logger logger = LoggerFactory.getLogger(BaseConsumerVerticle.class);

  private final Initializer initializer;
  final Set<String> topics;

  KafkaConsumer<Object, CloudEvent> consumer;
  RecordDispatcher recordDispatcher;
  private AsyncCloseable closeable;

  public BaseConsumerVerticle(final Initializer initializer, final Set<String> topics) {
    Objects.requireNonNull(topics);
    Objects.requireNonNull(initializer);

    this.topics = topics;
    this.initializer = initializer;
  }

  abstract void startConsumer(Promise<Void> startPromise);

  @Override
  public void start(Promise<Void> startPromise) {
    this.initializer.apply(vertx, this)
      .onFailure(startPromise::fail)
      .onSuccess(v -> {
        this.consumer.exceptionHandler(this::exceptionHandler);

        startConsumer(startPromise);
      });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    logger.info("Stopping consumer");

    AsyncCloseable
      .compose(this.recordDispatcher, this.closeable, this.consumer::close)
      .close(stopPromise);
  }

  public void setConsumer(KafkaConsumer<Object, CloudEvent> consumer) {
    this.consumer = consumer;
  }

  public void setRecordDispatcher(RecordDispatcher recordDispatcher) {
    this.recordDispatcher = recordDispatcher;
  }

  public void setCloser(AsyncCloseable closeable) {
    this.closeable = closeable;
  }

  void exceptionHandler(Throwable cause) {
    // TODO Add context (consumer group, resource id, etc)
    // TODO Send message on event bus
    logger.error("Consumer exception", cause);

    // Propagate exception to the verticle exception handler.
    if (context.exceptionHandler() != null) {
      this.context.exceptionHandler().handle(cause);
    }
  }

  public abstract PartitionRevokedHandler getPartitionsRevokedHandler();
}
