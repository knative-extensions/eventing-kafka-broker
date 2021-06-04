package dev.knative.eventing.kafka.broker.core.filter.impl;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import java.util.function.Function;

public abstract class BaseStringFilter implements Filter {

  protected final Function<CloudEvent, String> extractor;
  protected final String expectedValue;

  public BaseStringFilter(String attribute, String expectedValue) {
    if (attribute == null || attribute.isBlank()) {
      throw new IllegalArgumentException("Attribute name is empty");
    }
    if (expectedValue == null || expectedValue.isBlank()) {
      throw new IllegalArgumentException("Attribute value is empty");
    }

    this.extractor = AttributesFilter.attributesMapper.getOrDefault(attribute, e -> {
      Object extValue = e.getExtension(attribute);
      if (extValue == null) {
        return null;
      }
      return extValue.toString();
    });
    this.expectedValue = expectedValue;
  }

}
