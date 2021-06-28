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
package dev.knative.eventing.kafka.broker.dispatcher.impl.filter;

import dev.knative.eventing.kafka.broker.dispatcher.Filter;
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
