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
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleLoggingContext;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.Objects;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public abstract class ConsumerVerticle extends AbstractVerticle {


  public interface Initializer extends BiFunction<Vertx, ConsumerVerticle, Future<Void>> {
  }

  private static final Logger logger = LoggerFactory.getLogger(ConsumerVerticle.class);

  private final Initializer initializer;

  private final ConsumerVerticleContext consumerVerticleContext;
  private final ConsumerVerticleLoggingContext loggingContext;

  KafkaConsumer<Object, CloudEvent> consumer;
  RecordDispatcher recordDispatcher;
  private AsyncCloseable closeable;

  public ConsumerVerticle(final ConsumerVerticleContext consumerVerticleContext,
                          final Initializer initializer) {
    Objects.requireNonNull(consumerVerticleContext);

    this.consumerVerticleContext = consumerVerticleContext;
    this.loggingContext = new ConsumerVerticleLoggingContext(consumerVerticleContext);

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
    logger.info("Stopping consumer {}", keyValue("context", consumerVerticleContext));

    AsyncCloseable
      .compose(this.recordDispatcher, this.closeable, this.consumer::close)
      .close()
      .onComplete(r -> logger.info("Consumer verticle closed {}", keyValue("context", consumerVerticleContext)));

    stopPromise.tryComplete();
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
    logger.error("Consumer exception {}", keyValue("context", consumerVerticleContext), cause);

    // Propagate exception to the verticle exception handler.
    if (super.context.exceptionHandler() != null) {
      super.context.exceptionHandler().handle(cause);
    }
  }

  protected ConsumerVerticleContext getConsumerVerticleContext() {
    return consumerVerticleContext;
  }

  protected ConsumerVerticleLoggingContext getLoggingContext() {
    return loggingContext;
  }

  public abstract PartitionRevokedHandler getPartitionsRevokedHandler();
}
