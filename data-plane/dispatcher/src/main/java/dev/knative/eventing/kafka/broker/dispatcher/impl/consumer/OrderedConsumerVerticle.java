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

import dev.knative.eventing.kafka.broker.core.OrderedAsyncExecutor;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class OrderedConsumerVerticle extends BaseConsumerVerticle {

  private static final Logger logger = LoggerFactory.getLogger(OrderedConsumerVerticle.class);

  private static final long POLLING_MS = 200L;
  private static final Duration POLLING_TIMEOUT = Duration.ofMillis(1000L);

  private final Map<TopicPartition, OrderedAsyncExecutor> recordDispatcherExecutors;
  private final PartitionRevokedHandler partitionRevokedHandler;

  private final AtomicBoolean closed;
  private final AtomicLong pollTimer;
  private final AtomicBoolean isPollInFlight;

  public OrderedConsumerVerticle(Initializer initializer, Set<String> topics) {
    super(initializer, topics);
    this.recordDispatcherExecutors = new ConcurrentHashMap<>();
    this.closed = new AtomicBoolean(false);
    this.isPollInFlight = new AtomicBoolean(false);
    this.pollTimer = new AtomicLong(-1);

    partitionRevokedHandler = partitions -> {
      // Stop executors associated with revoked partitions.
      for (final TopicPartition partition : partitions) {
        final var executor = recordDispatcherExecutors.remove(partition);
        if (executor != null) {
          logger.info("Stopping executor {}", keyValue("topicPartition", partition));
          executor.stop();
        }
      }
      return Future.succeededFuture();
    };
  }

  @Override
  void startConsumer(Promise<Void> startPromise) {
    // We need to sub first, then we can start the polling loop
    this.consumer.subscribe(this.topics)
      .onFailure(startPromise::fail)
      .onSuccess(v -> {
        if (this.pollTimer.compareAndSet(-1, 0)) {
          this.pollTimer.set(vertx.setPeriodic(POLLING_MS, x -> poll()));
        }
        this.poll();
        startPromise.complete();
      });
  }

  private void poll() {
    if (this.closed.get() || this.isPollInFlight.get()) {
      logger.debug("Consumer closed or poll is in-flight {}", keyValue("topics", topics));
      return;
    }

    // Only poll new records when at-least one internal per-partition queue
    // needs more records.
    if (!isWaitingForTasks()) {
      logger.debug("all executors are busy {}", keyValue("topics", topics));
      return;
    }

    if (this.isPollInFlight.compareAndSet(false, true)) {
      logger.debug("Polling for records {}",
        keyValue("topics", topics)
      );

      this.consumer.poll(POLLING_TIMEOUT)
        .onSuccess(this::recordsHandler)
        .onFailure(t -> {
          if (this.closed.get()) {
            // The failure might have been caused by stopping the consumer, so we just ignore it
            return;
          }
          isPollInFlight.set(false);
          exceptionHandler(t);
        });
    }
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    // Stop the executors
    this.closed.set(true);
    this.vertx.cancelTimer(this.pollTimer.get());
    this.recordDispatcherExecutors.values().forEach(OrderedAsyncExecutor::stop);
    // Stop the consumer
    super.stop(stopPromise);
  }

  @Override
  public PartitionRevokedHandler getPartitionsRevokedHandler() {
    return partitionRevokedHandler;
  }

  private void recordsHandler(KafkaConsumerRecords<Object, CloudEvent> records) {
    if (this.closed.get()) {
      return;
    }
    isPollInFlight.set(false);

    if (records == null || records.size() == 0) {
      return;
    }
    // Put records in queues
    // I assume the records are ordered per topic-partition
    for (int i = 0; i < records.size(); i++) {
      final var record = records.recordAt(i);
      final var executor = executorFor(new TopicPartition(record.topic(), record.partition()));
      executor.offer(() -> dispatch(record));
    }
  }

  private Future<Void> dispatch(final KafkaConsumerRecord<Object, CloudEvent> record) {
    if (this.closed.get()) {
      return Future.failedFuture("Consumer verticle closed topics=" + topics);
    }
    return this.recordDispatcher.dispatch(record);
  }

  private synchronized OrderedAsyncExecutor executorFor(final TopicPartition topicPartition) {
    var executor = this.recordDispatcherExecutors.get(topicPartition);
    if (executor != null) {
      return executor;
    }
    executor = new OrderedAsyncExecutor();
    this.recordDispatcherExecutors.put(topicPartition, executor);
    return executor;
  }

  private boolean isWaitingForTasks() {
    for (OrderedAsyncExecutor value : this.recordDispatcherExecutors.values()) {
      if (value.isWaitingForTasks()) {
        return true;
      }
    }
    return this.recordDispatcherExecutors.size() == 0;
  }
}
