package dev.knative.eventing.kafka.broker.core.filter.impl;

import io.cloudevents.CloudEvent;

public class SuffixFilter extends BaseStringFilter {

  public SuffixFilter(String attribute, String expectedValue) {
    super(attribute, expectedValue);
  }

  @Override
  public boolean test(CloudEvent cloudEvent) {
    String value = this.extractor.apply(cloudEvent);
    return value != null && value.endsWith(this.expectedValue);
  }
}
