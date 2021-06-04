package dev.knative.eventing.kafka.broker.core.filter.impl;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import java.util.Set;

public class AllFilter implements Filter {

  private final Set<Filter> filters;

  public AllFilter(Set<Filter> filters) {
    this.filters = filters;
  }

  @Override
  public boolean test(CloudEvent cloudEvent) {
    for (Filter filter : filters) {
      if (!filter.test(cloudEvent)) {
        return false;
      }
    }
    return true;
  }
}
