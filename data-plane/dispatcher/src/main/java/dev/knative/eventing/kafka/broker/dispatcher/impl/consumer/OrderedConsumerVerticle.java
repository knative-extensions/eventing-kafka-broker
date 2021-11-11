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
import io.vertx.core.Promise;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderedConsumerVerticle extends BaseConsumerVerticle {

  private static final Logger logger = LoggerFactory.getLogger(OrderedConsumerVerticle.class);
  private static final Duration POLLING_TIMEOUT = Duration.ofMillis(100);

  // poll wait ms = (POLL_WAIT_RECORD_FACTOR * pendingRecords) / (partitions - POLL_WAIT_PARTITION_FACTOR * Math.sqrt(partitions))
  // Each record takes 10 ms to be processed
  private static final double POLL_WAIT_RECORD_FACTOR = 10;
  // This is used to take in account how much the parallelism is effective
  private static final double POLL_WAIT_PARTITION_FACTOR = 1 / (double) Runtime.getRuntime().availableProcessors();
  private static final long MAX_POLL_WAIT = 10 * 1000;
  private static final int MIN_POLL_WAIT = 100;

  private final Map<TopicPartition, OrderedAsyncExecutor> recordDispatcherExecutors;

  private int pendingRecords;
  private boolean stopPolling;

  public OrderedConsumerVerticle(Initializer initializer, Set<String> topics) {
    super(initializer, topics);

    this.recordDispatcherExecutors = new HashMap<>();

    this.pendingRecords = 0;
    this.stopPolling = false;
  }

  @Override
  void startConsumer(Promise<Void> startPromise) {
    this.consumer.exceptionHandler(this::exceptionHandler);
    // We need to sub first, then we can start the polling loop
    this.consumer.subscribe(this.topics)
      .onFailure(startPromise::fail)
      .onSuccess(v -> {
        startPromise.complete();

        logger.debug("Starting polling");
        this.poll();
      });
  }

  public void poll() {
    if (this.stopPolling) {
      return;
    }
    this.consumer.poll(POLLING_TIMEOUT)
      .onFailure(t -> {
        if (this.stopPolling) {
          // The failure might have been caused by stopping the consumer, so we just ignore it
          return;
        }
        this.exceptionHandler(t);
        this.schedulePoll();
      })
      .onSuccess(records -> {
        this.recordsHandler(records);
        this.schedulePoll();
      });
  }

  void schedulePoll() {
    // Check if we need to poll immediately, or wait a bit for the queues to free
    long pollWait = pollWaitMs();

    if (pollWait > MIN_POLL_WAIT) {
      vertx.setTimer(pollWaitMs(), v -> this.poll());
    } else {
      // Poll immediately
      this.poll();
    }
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    // Stop the executors
    this.stopPolling = true;
    this.recordDispatcherExecutors.values().forEach(OrderedAsyncExecutor::stop);

    // Stop the consumer
    super.stop(stopPromise);
  }

  void recordsHandler(KafkaConsumerRecords<String, CloudEvent> records) {
    if (records == null) {
      return;
    }
    // Put records in queues
    // I assume the records are ordered per topic-partition
    for (int i = 0; i < records.size(); i++) {
      this.pendingRecords++;
      KafkaConsumerRecord<String, CloudEvent> record = records.recordAt(i);
      this.enqueueRecord(new TopicPartition(record.topic(), record.partition()), record);
    }
  }

  void enqueueRecord(TopicPartition topicPartition, KafkaConsumerRecord<String, CloudEvent> record) {
    this.recordDispatcherExecutors.computeIfAbsent(topicPartition, (tp) -> new OrderedAsyncExecutor())
      .offer(() -> this.recordDispatcher.dispatch(record).onComplete(v -> this.pendingRecords--));
  }

  long pollWaitMs() {
    double partitions = this.recordDispatcherExecutors.size();
    long computed = Math.round(
      (POLL_WAIT_RECORD_FACTOR * pendingRecords) /
        (partitions - (POLL_WAIT_PARTITION_FACTOR * Math.sqrt(partitions)))
    );
    return Math.min(computed, MAX_POLL_WAIT);
  }

}
