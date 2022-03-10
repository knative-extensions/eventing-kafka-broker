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
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AllFilter implements Filter {

  private final Set<Filter> filters;
  private static final Logger logger = LoggerFactory.getLogger(AllFilter.class);


  public AllFilter(Set<Filter> filters) {
    this.filters = filters;
  }

  @Override
  public boolean test(CloudEvent cloudEvent) {
    logger.debug("Testing event against ALL filters. Event {}", cloudEvent);
    for (Filter filter : filters) {
      if (!filter.test(cloudEvent)) {
        logger.debug("Test failed. Filter {} Event {}", filter, cloudEvent);
        return false;
      }
    }
    logger.debug("Test ALL filters succeeded. Event {}", cloudEvent);
    return true;
  }
}
