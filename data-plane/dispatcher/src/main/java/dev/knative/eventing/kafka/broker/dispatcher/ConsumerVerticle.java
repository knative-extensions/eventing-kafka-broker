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

import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This class is responsible for managing the consumer lifecycle.
 */
public final class ConsumerVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerVerticle.class);

  private KafkaConsumer<String, CloudEvent> consumer;
  private AutoCloseable consumerMeterBinder;
  private RecordDispatcher handler;

  private final Set<String> topics;
  private final Function<Vertx, Future<KafkaConsumer<String, CloudEvent>>> consumerFactory;
  private final BiFunction<Vertx, KafkaConsumer<String, CloudEvent>, Future<RecordDispatcher>>
    recordHandlerFactory;

  /**
   * All args constructor.
   *
   * @param consumerFactory      Kafka consumer.
   * @param topics               topic to consume.
   * @param recordHandlerFactory record handler factory.
   */
  public ConsumerVerticle(
    final Function<Vertx, Future<KafkaConsumer<String, CloudEvent>>> consumerFactory,
    final Set<String> topics,
    final BiFunction<Vertx, KafkaConsumer<String, CloudEvent>, Future<RecordDispatcher>> recordHandlerFactory) {

    Objects.requireNonNull(consumerFactory, "provide consumerFactory");
    Objects.requireNonNull(topics, "provide topic");
    Objects.requireNonNull(recordHandlerFactory, "provide recordHandlerFactory");

    this.topics = topics;
    this.recordHandlerFactory = recordHandlerFactory;
    this.consumerFactory = consumerFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start(Promise<Void> startPromise) {
    consumerFactory.apply(vertx)
      .onSuccess(consumer -> {
        if (consumer == null) {
          startPromise.fail("Consumer cannot be null");
          return;
        }

        this.consumer = consumer;
        this.consumerMeterBinder = Metrics.register(this.consumer.unwrap());
        recordHandlerFactory.apply(vertx, this.consumer)
          .onSuccess(h -> {
            this.handler = h;
            this.consumer.handler(this.handler);
            consumer.exceptionHandler(this.exceptionHandler(startPromise));
            this.consumer.subscribe(this.topics, startPromise);
          })
          .onFailure(startPromise::tryFail);
      })
      .onFailure(startPromise::fail);
  }

  private Handler<Throwable> exceptionHandler(final Promise<Void> startPromise) {
    return cause -> {
      // TODO Add context (consumer group, resource id, etc)
      // TODO Send message on event bus
      logger.error("Consumer exception", cause);
      startPromise.tryFail(cause);
      this.context.exceptionHandler().handle(cause);
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop(Promise<Void> stopPromise) {
    logger.info("Stopping consumer");

    CompositeFuture.all(
      this.consumer.close(),
      this.handler.close(),
      Metrics.close(vertx, consumerMeterBinder)
    )
      .onSuccess(r -> stopPromise.complete())
      .onFailure(stopPromise::fail);
  }
}
