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

import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import io.cloudevents.CloudEvent;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnyFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(AnyFilter.class);

    private final List<FilterCounter> filters;

    private final ArrayBlockingQueue<Integer> indexSwapQueue;

    private final FilterListOptimizer filterListOptimizer;

    private final ReadWriteLock readWriteLock;

    public AnyFilter(List<Filter> filters) {
        this.filters = filters.stream().map(FilterCounter::new).collect(Collectors.toList());
        this.indexSwapQueue = new ArrayBlockingQueue<>(1);
        this.readWriteLock = new ReentrantReadWriteLock();
        this.filterListOptimizer =
                new FilterListOptimizer(this.readWriteLock, this.indexSwapQueue, this.filters, logger);
        this.filterListOptimizer.start();
    }

    @Override
    public boolean test(CloudEvent cloudEvent) {
        logger.debug("Testing event against ANY filter. Event {}", cloudEvent);
        this.readWriteLock.readLock().lock();
        for (int i = 0; i < this.filters.size(); i++) {
            Filter filter = this.filters.get(i).getFilter();
            if (filter.test(cloudEvent)) {
                this.indexSwapQueue.offer(i);
                logger.debug("Test succeeded. Filter {} Event {}", filter, cloudEvent);
                this.readWriteLock.readLock().unlock();
                return true;
            }
        }
        logger.debug("Test failed. All filters failed. Event {}", cloudEvent);
        this.readWriteLock.readLock().unlock();
        return false;
    }

    @Override
    public void close() {
        this.filterListOptimizer.interrupt();
    }
}
