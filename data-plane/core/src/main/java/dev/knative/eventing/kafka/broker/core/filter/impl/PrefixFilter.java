package dev.knative.eventing.kafka.broker.core.filter.impl;

import io.cloudevents.CloudEvent;

public class PrefixFilter extends BaseStringFilter {

  public PrefixFilter(String attribute, String expectedValue) {
    super(attribute, expectedValue);
  }

  @Override
  public boolean test(CloudEvent cloudEvent) {
    String value = this.extractor.apply(cloudEvent);
    return value != null && value.startsWith(this.expectedValue);
  }
}
