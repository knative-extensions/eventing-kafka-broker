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
package dev.knative.eventing.kafka.broker.core.filter.subscriptionsapi;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnyFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(AnyFilter.class);

    private final List<Filter> filters;

    public static Filter newFilter(List<Filter> filters) {
        if (filters.size() == 1) {
            return filters.getFirst();
        }

        return new AnyFilter(filters);
    }

    private AnyFilter(List<Filter> filters) {
        this.filters = filters;
    }

    @Override
    public boolean test(CloudEvent cloudEvent) {
        logger.debug("Testing event against ANY filter. Event {}", cloudEvent);

        for (Filter filter : filters) {
            if (filter.test(cloudEvent)) {
                logger.debug("Test succeeded. Filter {} Event {}", filter, cloudEvent);
                return true;
            }
        }
        logger.debug("Test failed. All filters failed. Event {}", cloudEvent);
        return false;
    }
}
