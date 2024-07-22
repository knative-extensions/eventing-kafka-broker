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
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(NotFilter.class);

    private final Filter filter;

    private int count;

    public NotFilter(Filter filter) {
        this.filter = filter;
        this.count = 0;
    }

    @Override
    public boolean test(CloudEvent cloudEvent) {
        logger.debug("Testing NOT filter. Event {}", cloudEvent);
        boolean passed = !filter.test(cloudEvent);
        String result = passed ? "Succeeded" : "Failed";
        logger.debug("{}: Filter {} - Event {}", result, this.filter, cloudEvent);
        return passed;
    }

    @Override
    public int getCount() {
        return this.count;
    }

    @Override
    public int incrementCount() {
        return this.count++;
    }

    @Override
    public void close(Vertx vertx) {
        this.filter.close(vertx);
    }
}
