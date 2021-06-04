package dev.knative.eventing.kafka.broker.core.filter.impl;

import io.cloudevents.CloudEvent;

public class ExactFilter extends BaseStringFilter {

  public ExactFilter(String attribute, String expectedValue) {
    super(attribute, expectedValue);
  }

  @Override
  public boolean test(CloudEvent cloudEvent) {
    return expectedValue.equals(this.extractor.apply(cloudEvent));
  }
}
