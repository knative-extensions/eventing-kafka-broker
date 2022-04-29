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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

public class OrderedConsumerVerticle extends BaseConsumerVerticle {

  private static final Logger logger = LoggerFactory.getLogger(OrderedConsumerVerticle.class);

  private static final long POLLING_MS = 200L;
  private static final Duration POLLING_TIMEOUT = Duration.ofMillis(1000L);

  private final Map<TopicPartition, OrderedAsyncExecutor> recordDispatcherExecutors;

  private boolean closed;
  private long pollPeriodicTimer;

  public OrderedConsumerVerticle(Initializer initializer, Set<String> topics) {
    super(initializer, topics);
    this.recordDispatcherExecutors = new HashMap<>();
    this.closed = false;
  }

  @Override
  void startConsumer(Promise<Void> startPromise) {
    // We need to sub first, then we can start the polling loop
    this.consumer.subscribe(this.topics)
      .onFailure(startPromise::fail)
      .onSuccess(v -> {
        startPromise.complete();
        this.poll(true);
        this.pollPeriodicTimer = vertx.setPeriodic(POLLING_MS, x -> poll(false));
      });
  }

  private void poll(final boolean force) {
    if (this.closed) {
      return;
    }

    logger.debug("Polling for records {} {} {}", keyValue("topics", topics), keyValue("force", force), keyValue("isWaitingForTasks", waitingForTasks()));

    if (!force && !waitingForTasks()) {
      return;
    }

    logger.debug("Polling for records {}", keyValue("topics", topics));

    this.consumer.poll(POLLING_TIMEOUT)
      .onSuccess(this::recordsHandler)
      .onFailure(t -> {
        if (this.closed) {
          // The failure might have been caused by stopping the consumer, so we just ignore it
          return;
        }
        exceptionHandler(t);
      });
  }

  private boolean waitingForTasks() {
    for (OrderedAsyncExecutor value : recordDispatcherExecutors.values()) {
      if (value.isWaitingForTasks()) {
        return true;
      }
    }
    return recordDispatcherExecutors.size() == 0;
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    // Stop the executors
    this.closed = true;
    this.vertx.cancelTimer(this.pollPeriodicTimer);
    this.recordDispatcherExecutors.values().forEach(OrderedAsyncExecutor::stop);
    // Stop the consumer
    super.stop(stopPromise);
  }

  private void recordsHandler(KafkaConsumerRecords<Object, CloudEvent> records) {
    if (this.closed) {
      return;
    }
    if (records == null || records.size() == 0) {
      return;
    }
    // Put records in queues
    // I assume the records are ordered per topic-partition
    for (int i = 0; i < records.size(); i++) {
      final var record = records.recordAt(i);
      final var executor = executorFor(new TopicPartition(record.topic(), record.partition()));
      executor.offer(() -> dispatch(record, executor));
    }
  }

  private Future<Void> dispatch(final KafkaConsumerRecord<Object, CloudEvent> record,
                                final OrderedAsyncExecutor executor) {
    if (this.closed) {
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
}
