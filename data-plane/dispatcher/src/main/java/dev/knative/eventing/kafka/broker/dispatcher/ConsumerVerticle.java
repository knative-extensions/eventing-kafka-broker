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
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConsumerVerticle is responsible for manging the consumer lifecycle.
 *
 * @param <K> record key type.
 * @param <V> record value type.
 */
public final class ConsumerVerticle<K, V, R> extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerVerticle.class);

  private KafkaConsumer<K, V> consumer;
  private AutoCloseable consumerMeterBinder;
  private ConsumerRecordHandler<K, V, R> handler;

  private final Set<String> topics;
  private final Function<Vertx, KafkaConsumer<K, V>> consumerFactory;
  private final BiFunction<Vertx, KafkaConsumer<K, V>, ConsumerRecordHandler<K, V, R>> recordHandler;

  /**
   * All args constructor.
   *
   * @param consumerFactory      Kafka consumer.
   * @param topics               topic to consume.
   * @param recordHandlerFactory record handler factory.
   */
  public ConsumerVerticle(
    final Function<Vertx, KafkaConsumer<K, V>> consumerFactory,
    final Set<String> topics,
    final BiFunction<Vertx, KafkaConsumer<K, V>, ConsumerRecordHandler<K, V, R>> recordHandlerFactory) {

    Objects.requireNonNull(consumerFactory, "provide consumerFactory");
    Objects.requireNonNull(topics, "provide topic");
    Objects.requireNonNull(recordHandlerFactory, "provide recordHandlerFactory");

    this.topics = topics;
    this.recordHandler = recordHandlerFactory;
    this.consumerFactory = consumerFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start(Promise<Void> startPromise) {
    this.consumer = consumerFactory.apply(vertx);
    this.consumerMeterBinder = Metrics.register(this.consumer.unwrap());
    this.handler = recordHandler.apply(vertx, this.consumer);

    consumer.handler(handler);
    consumer.exceptionHandler(startPromise::tryFail);
    consumer.subscribe(topics, startPromise);
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
