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
package dev.knative.eventing.kafka.broker.dispatcher.impl.filter.subscriptionsapi;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import org.slf4j.Logger;

public class FilterListOptimizer extends Thread {
    private final ReadWriteLock readWriteLock;

    private final ArrayBlockingQueue<Integer> indexSwapQueue;

    private final List<FilterCounter> filters;

    private final Logger logger;

    public FilterListOptimizer(
            ReadWriteLock readWriteLock,
            ArrayBlockingQueue<Integer> indexSwapQueue,
            List<FilterCounter> filters,
            Logger logger) {
        this.filters = filters;
        this.indexSwapQueue = indexSwapQueue;
        this.readWriteLock = readWriteLock;
        this.logger = logger;
    }

    @Override
    public void run() {
        while (true) {
            if (Thread.interrupted()) {
                return;
            }
            try {
                this.readWriteLock.readLock().lock();
                final int swapIndex =
                        this.indexSwapQueue.take(); // this is the only line that throws InterruptedException
                if (swapIndex != 0
                        && this.filters.get(swapIndex).incrementCount()
                                > 2 * this.filters.get(swapIndex - 1).getCount()) {
                    new Thread(() -> {
                                this.readWriteLock.writeLock().lock();
                                Collections.swap(this.filters, swapIndex - 1, swapIndex);
                                this.readWriteLock.writeLock().unlock();
                            })
                            .start();
                }
                this.readWriteLock.readLock().unlock();
            } catch (InterruptedException e) {
                logger.debug("Filter optimizer thread was interrupted", e);
                this.readWriteLock.readLock().unlock();
            }
        }
    }
}
