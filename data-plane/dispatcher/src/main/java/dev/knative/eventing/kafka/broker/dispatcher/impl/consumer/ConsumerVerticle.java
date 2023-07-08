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
import dev.knative.eventing.kafka.broker.dispatcher.ReactiveKafkaConsumer;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;
import io.cloudevents.CloudEvent;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public abstract class ConsumerVerticle extends AbstractVerticle {


  public interface Initializer extends BiFunction<Vertx, ConsumerVerticle, Future<Void>> {
  }

  private static final Logger logger = LoggerFactory.getLogger(ConsumerVerticle.class);

  private final Initializer initializer;

  private final ConsumerVerticleContext consumerVerticleContext;

  private final Collection<PartitionRevokedHandler> partitionRevokedHandlers;

  ReactiveKafkaConsumer<Object, CloudEvent> consumer;
  RecordDispatcher recordDispatcher;
  private AsyncCloseable closeable;

  public ConsumerVerticle(final ConsumerVerticleContext consumerVerticleContext,
                          final Initializer initializer) {
    Objects.requireNonNull(consumerVerticleContext);

    this.consumerVerticleContext = consumerVerticleContext;
    this.initializer = initializer;
    this.partitionRevokedHandlers = new ArrayList<>();
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
    logger.info("Stopping consumer {}", consumerVerticleContext.getLoggingKeyValue());

    AsyncCloseable
      .compose(this.recordDispatcher, this.closeable, this.consumer::close)
      .close()
      .onComplete(r -> logger.info("Consumer verticle closed {}", consumerVerticleContext.getLoggingKeyValue()));

    stopPromise.tryComplete();
  }

  public void setConsumer(ReactiveKafkaConsumer<Object, CloudEvent> consumer) {
    this.consumer = consumer;
  }

  public void addPartitionRevokedHandler(PartitionRevokedHandler partitionRevokedHandler) {
    this.partitionRevokedHandlers.add(partitionRevokedHandler);
  }

  public void setRecordDispatcher(RecordDispatcher recordDispatcher) {
    this.recordDispatcher = recordDispatcher;
  }

  public void setCloser(AsyncCloseable closeable) {
    this.closeable = closeable;
  }

  void exceptionHandler(Throwable cause) {
    logger.error("Consumer exception {}", consumerVerticleContext.getLoggingKeyValue(), cause);

    // Propagate exception to the verticle exception handler.
    if (super.context.exceptionHandler() != null) {
      super.context.exceptionHandler().handle(cause);
    }
  }

  protected ConsumerVerticleContext getConsumerVerticleContext() {
    return consumerVerticleContext;
  }

  protected ConsumerRebalanceListener getConsumerRebalanceListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
        ConsumerVerticleContext.logger.info("Received revoke partitions for consumer {} {}",
                consumerVerticleContext.getLoggingKeyValue(),
                keyValue("partitions", partitions)
        );

        final var futures = new ArrayList<Future<Void>>(partitionRevokedHandlers.size());
        for (PartitionRevokedHandler partitionRevokedHandler : partitionRevokedHandlers) {
            futures.add(partitionRevokedHandler.partitionRevoked(partitions));
        }

        for (final var future : futures) {
            try {
                future.toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
            } catch (final Exception ignored) {
                ConsumerVerticleContext.logger.warn("Partition revoked handler failed {} {}",
                        consumerVerticleContext.getLoggingKeyValue(),
                        keyValue("partitions", partitions)
                );
            }
        }
      }

      @Override
      public void onPartitionsAssigned(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
        ConsumerVerticleContext.logger.info("Received assign partitions for consumer {} {}",
                consumerVerticleContext.getLoggingKeyValue(),
                keyValue("partitions", partitions)
        );
      }
    };
  }

  public abstract PartitionRevokedHandler getPartitionRevokedHandler();
  
}
