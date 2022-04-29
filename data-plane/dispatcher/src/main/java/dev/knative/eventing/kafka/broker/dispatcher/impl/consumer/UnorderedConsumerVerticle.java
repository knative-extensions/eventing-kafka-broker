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

import dev.knative.eventing.kafka.broker.dispatcher.DeliveryOrder;
import io.cloudevents.CloudEvent;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

/**
 * This {@link io.vertx.core.Verticle} implements an unordered consumer logic, as described in {@link DeliveryOrder#UNORDERED}.
 */
public final class UnorderedConsumerVerticle extends BaseConsumerVerticle {

  private static final Logger logger = LoggerFactory.getLogger(UnorderedConsumerVerticle.class);

  private static final long BACKOFF_DELAY_MS = 200;
  // This shouldn't be more than 2000, which is the default max time allowed
  // to block a verticle thread.
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

  private final int maxPollRecords;

  private boolean closed;
  private int inFlightRecords;
  private boolean isPollInFlight;
  private long pollPeriodicTimer;

  public UnorderedConsumerVerticle(final Initializer initializer,
                                   final Set<String> topics,
                                   final int maxPollRecords) {
    super(initializer, topics);
    if (maxPollRecords <= 0) {
      this.maxPollRecords = 500;
    } else {
      this.maxPollRecords = maxPollRecords;
    }
    this.inFlightRecords = 0;
    this.closed = false;
    this.isPollInFlight = false;
  }

  @Override
  void startConsumer(Promise<Void> startPromise) {
    this.consumer.subscribe(this.topics, startPromise);

    startPromise.future()
      .onSuccess(v -> {
        poll();
        this.pollPeriodicTimer = vertx.setPeriodic(BACKOFF_DELAY_MS, x -> poll());
      });
  }

  /**
   * Vert.x auto-subscribe and handling of records might grow
   * unbounded, and it is particularly evident when the consumer
   * is slow to consume messages.
   * <p>
   * To apply backpressure, we need to bound the number of outbound
   * in-flight requests, so we need to manually poll for new records
   * as we dispatch them to the subscriber service.
   * <p>
   * The maximum number of outbound in-flight requests is already configurable
   * with the consumer parameter `max.poll.records`, and it's critical to
   * control the memory consumption of the dispatcher.
   */
  private synchronized void poll() {
    if (closed || isPollInFlight) {
      return;
    }
    if (inFlightRecords >= maxPollRecords) {
      logger.debug(
        "In flight records exceeds " + ConsumerConfig.MAX_POLL_RECORDS_CONFIG +
          " waiting for response from subscriber before polling for new records {} {} {}",
        keyValue(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords),
        keyValue("records", inFlightRecords),
        keyValue("topics", topics)
      );
      return;
    }
    isPollInFlight = true;
    this.consumer
      .poll(POLL_TIMEOUT)
      .onSuccess(this::handleRecords)
      .onFailure(cause -> {
        isPollInFlight = false;
        logger.error("Failed to poll messages {}", keyValue("topics", topics), cause);
      });
  }

  private void handleRecords(final KafkaConsumerRecords<Object, CloudEvent> records) {
    if (closed) {
      isPollInFlight = false;
      return;
    }
    if (records == null || records.size() == 0) {
      isPollInFlight = false;
      return;
    }

    // We are not forcing the dispatcher to send less than `max.poll.records`
    // requests because we don't want to keep records in-memory by waiting
    // for responses.
    this.inFlightRecords += records.size();
    isPollInFlight = false;

    for (int i = 0; i < records.size(); i++) {
      this.recordDispatcher.dispatch(records.recordAt(i))
        .onComplete(v -> this.inFlightRecords--);
    }
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    this.closed = true;
    vertx.cancelTimer(pollPeriodicTimer);
    // Stop the consumer
    super.stop(stopPromise);
  }
}
