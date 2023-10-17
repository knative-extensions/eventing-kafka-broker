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

import com.google.common.collect.ImmutableList;
import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import io.cloudevents.CloudEvent;
import io.vertx.core.Vertx;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnyFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(AnyFilter.class);

    private final AtomicReference<ImmutableList<Filter>> filters;

    private final AtomicInteger count;

    private final long periodicTimerId;

    private boolean shouldReorder;

    public AnyFilter(List<Filter> filters, Vertx vertx, long delayMilliseconds) {
        this.periodicTimerId = vertx.setPeriodic(delayMilliseconds, this::reorder);
        this.count = new AtomicInteger(0);
        this.filters = new AtomicReference<>(filters.stream().collect(ImmutableList.toImmutableList()));
    }

    private void reorder(Long id) {
        if (!this.shouldReorder) {
            return;
        }
        logger.debug("Reordering ANY filter!");
        this.filters.set(this.filters.get().stream()
                .sorted(Comparator.comparingInt(Filter::getCount).reversed())
                .collect(ImmutableList.toImmutableList()));
        this.shouldReorder = false;
    }

    private static boolean test(CloudEvent cloudEvent, ImmutableList<Filter> filters, Consumer<Boolean> shouldReorder) {
        logger.debug("Testing event against ANY filter. Event {}", cloudEvent);
        int i = 0;
        for (final Filter filter : filters) {
            if (filter.test(cloudEvent)) {
                int count = filter.incrementCount();
                if (i != 0 && count > 2 * filters.get(i - 1).getCount()) {
                    shouldReorder.accept(true);
                }
                logger.debug("Test succeeded. Filter {} Event {}", filter, cloudEvent);
                return true;
            }
            i++;
        }
        logger.debug("Test failed. All filters failed. Event {}", cloudEvent);
        return false;
    }

    @Override
    public int getCount() {
        return this.count.get();
    }

    @Override
    public int incrementCount() {
        return this.count.incrementAndGet();
    }

    private void setShouldReorder(boolean shouldReorder) {
        this.shouldReorder = shouldReorder;
    }

    @Override
    public boolean test(CloudEvent cloudEvent) {
        return AnyFilter.test(cloudEvent, this.filters.get(), this::setShouldReorder);
    }

    @Override
    public void close(Vertx vertx) {
        vertx.cancelTimer(this.periodicTimerId);
        this.filters.get().forEach((f) -> f.close(vertx));
    }
}
