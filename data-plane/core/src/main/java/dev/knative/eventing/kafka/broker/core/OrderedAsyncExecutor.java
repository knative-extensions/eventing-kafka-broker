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
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * This executor performs an ordered execution of the enqueued tasks.
 * <p>
 * This class assumes its execution is tied to a single verticle, hence it cannot be shared among verticles.
 */
public class OrderedAsyncExecutor {

  private final Queue<Supplier<Future<?>>> queue;

  private boolean isStopped;

  public OrderedAsyncExecutor() {
    this.queue = new ArrayDeque<>();
    this.isStopped = false;
  }

  /**
   * Offer a new task to the executor. The executor will start the task as soon as possible.
   *
   * @param task the task to offer
   */
  public void offer(Supplier<Future<?>> task) {
    boolean wasEmpty = queue.isEmpty();
    queue.offer(task);
    if (wasEmpty) { // If no elements in the queue, then we need to start consuming it
      consume();
    }
  }

  /**
   * Stop the executor. This won't stop the actual task on-fly, but it will prevent queued tasks to be executed.
   */
  public void stop() {
    this.isStopped = true;
  }

  void consume() {
    if (this.isStopped) {
      return;
    }
    Supplier<Future<?>> task = this.queue.peek();
    if (task == null) {
      return; // No task to process
    }
    task.get()
      .onComplete(ar -> {
        // We don't actually care about the result,
        // the task should have the failure handling by itself

        // Remove the element from the queue
        queue.poll();

        consume();
      });
  }

}
