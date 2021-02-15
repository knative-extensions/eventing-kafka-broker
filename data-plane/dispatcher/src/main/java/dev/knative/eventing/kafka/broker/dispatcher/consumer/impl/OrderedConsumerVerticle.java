package dev.knative.eventing.kafka.broker.dispatcher.consumer.impl;

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

  // If pendingRecords > LOAD_THRESHOLD, we'll wait for polling, otherwise we'll poll immediately
  private static final int LOAD_THRESHOLD = 100;
  // poll wait ms = POLL_WAIT_FACTOR * pendingRecords
  private static final long POLL_WAIT_FACTOR = 5;

  private static final Logger logger = LoggerFactory.getLogger(OrderedConsumerVerticle.class);
  private static final Duration POLLING_TIMEOUT = Duration.ofMillis(100);

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
    if (this.pendingRecords > LOAD_THRESHOLD) {
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
      .offer(() -> this.recordDispatcher.apply(record).onComplete(v -> this.pendingRecords--));
  }

  long pollWaitMs() {
    return POLL_WAIT_FACTOR * this.pendingRecords;
  }

}
