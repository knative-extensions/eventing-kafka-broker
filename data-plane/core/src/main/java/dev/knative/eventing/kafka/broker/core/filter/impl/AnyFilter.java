package dev.knative.eventing.kafka.broker.core.filter.impl;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import java.util.Set;

public class AnyFilter implements Filter {

  private final Set<Filter> filters;

  public AnyFilter(Set<Filter> filters) {
    this.filters = filters;
  }

  @Override
  public boolean test(CloudEvent cloudEvent) {
    for (Filter filter : filters) {
      if (filter.test(cloudEvent)) {
        return true;
      }
    }
    return false;
  }
}
