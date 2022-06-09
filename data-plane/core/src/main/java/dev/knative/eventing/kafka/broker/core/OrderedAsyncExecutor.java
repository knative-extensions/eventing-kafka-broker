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
package dev.knative.eventing.kafka.broker.core;

import io.vertx.core.Future;
import io.vertx.kafka.client.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

import static dev.knative.eventing.kafka.broker.core.utils.Logging.keyValue;

/**
 * This executor performs an ordered execution of the enqueued tasks.
 * <p>
 * This class assumes its execution is tied to a single verticle, hence it cannot be shared among verticles.
 */
public class OrderedAsyncExecutor {

  private static final Logger logger = LoggerFactory.getLogger(OrderedAsyncExecutor.class);

  private final Queue<Supplier<Future<?>>> queue;
  private final TopicPartition topicPartition;

  private boolean isStopped;
  private boolean inFlight;

  public OrderedAsyncExecutor(final TopicPartition topicPartition) {
    this.queue = new ConcurrentLinkedDeque<>();
    this.isStopped = false;
    this.inFlight = false;
    this.topicPartition = topicPartition;
  }

  /**
   * Offer a new task to the executor. The executor will start the task as soon as possible.
   *
   * @param task the task to offer
   */
  public void offer(Supplier<Future<?>> task) {
    if (this.isStopped) {
      // Executor is stopped, return without adding the task to the queue.
      return;
    }
    this.queue.offer(task);
    consume();
  }

  private void consume() {
    if (queue.isEmpty() || this.inFlight || this.isStopped) {
      return;
    }

    logger.debug("Consuming from queue {} {}",
      keyValue("topicPartition", topicPartition),
      keyValue("length", queue.size())
    );

    this.inFlight = true;
    this.queue
      .remove()
      .get()
      .onComplete(ar -> {
        this.inFlight = false;
        consume();
      });
  }

  public boolean isWaitingForTasks() {
    // TODO To perform this flag would be nice to take into account also the time that it takes for a sink to process a
    //  message so that we can fetch records in advance and keep queues busy.
    return this.queue.isEmpty();
  }

  /**
   * Stop the executor. This won't stop the actual in-flight task, but it will prevent queued tasks to be executed.
   */
  public void stop() {
    this.isStopped = true;
    this.queue.clear();
  }
}
