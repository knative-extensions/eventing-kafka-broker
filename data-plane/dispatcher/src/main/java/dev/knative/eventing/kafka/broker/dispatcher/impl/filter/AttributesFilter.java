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
import io.cloudevents.core.v03.CloudEventV03;
import io.cloudevents.core.v1.CloudEventV1;
import io.cloudevents.lang.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public abstract class AttributesFilter implements Filter {

  static class AttributeEntry {
    private final String name;
    private final String expectedValue;
    // An extractor function to turn an event into a string value.
    // specversion -> 1.0
    // f(event) -> event.getSpecVersion().toString() -> 1.0
    private final Function<CloudEvent, String> extractor;

    public AttributeEntry(String name, String expectedValue, Function<CloudEvent, String> extractor) {
      this.name = name;
      this.expectedValue = expectedValue;
      this.extractor = extractor;
    }
  }

  private static final String DEFAULT_STRING = "";

  protected final Logger logger = LoggerFactory.getLogger(getClass());


  static final Map<String, Function<CloudEvent, String>> attributesMapper = Map.of(
    CloudEventV1.SPECVERSION, event -> event.getSpecVersion().toString(),
    CloudEventV1.ID, CloudEvent::getId,
    CloudEventV1.TYPE, CloudEvent::getType,
    CloudEventV1.SOURCE, event -> event.getSource().toString(),
    CloudEventV1.DATACONTENTTYPE, CloudEvent::getDataContentType,
    CloudEventV1.DATASCHEMA, event -> getOrDefault(event.getDataSchema(), URI::toString),
    CloudEventV03.SCHEMAURL, event -> getOrDefault(event.getDataSchema(), URI::toString),
    CloudEventV1.SUBJECT, CloudEvent::getSubject,
    CloudEventV1.TIME, event -> getOrDefault(event.getTime(), time -> time.format(ISO_INSTANT))
  );

  private final List<AttributeEntry> attributes;

  /**
   * All args constructor.
   *
   * @param attributes attributes to match to pass filter.
   */
  public AttributesFilter(final Map<String, String> attributes) {
    this.attributes = attributes.entrySet().stream()
      .filter(entry -> isNotEmpty(entry.getValue()))
      .map(entry -> new AttributeEntry(
        entry.getKey(),
        entry.getValue(),
        attributesMapper.getOrDefault(
          entry.getKey(),
          event -> {
            try {
              return getOrDefault(event.getAttribute(entry.getKey()), Object::toString);
            } catch (Exception ex) {
              return getOrDefault(event.getExtension(entry.getKey()), Object::toString);
            }
          }
        )))
      .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Attributes filters events by exact match on event context attributes. Each key in the map is compared with the
   * equivalent key in the event context. An event passes the filter if all values are equal to the specified values.
   * Nested context attributes are not supported as keys. Only string values are supported.
   *
   * @param event event to match
   * @return true if event matches attributes, otherwise false.
   */
  @Override
  public boolean test(final CloudEvent event) {
    logger.debug("Testing event attributes. Event {}", event);
    for (final var entry : attributes) {
      Function<CloudEvent, String> extractorFunc = entry.extractor;
      String wantedValue = entry.expectedValue;
      String existingValue = extractorFunc.apply(event);
      if (!this.match(existingValue, wantedValue)) {
        logger.debug("Event attributes matching failed. Attribute: {} Want: {} Got: {} Event: {}",
          entry.name, wantedValue, existingValue, event);
        return false;
      }
    }
    logger.debug("Event attributes matching succeeded");
    return true;
  }


  /**
   * Matches the given value against the wanted value.
   *
   * @param wanted desired attribute value
   * @param given  cloud event current attribute value
   * @return
   */
  protected abstract boolean match(String given, String wanted);

  private static <T> String getOrDefault(
    @Nullable final T s,
    final Function<T, String> stringProvider) {

    if (s == null) {
      return DEFAULT_STRING;
    }
    return stringProvider.apply(s);
  }

  private static boolean isNotEmpty(final String value) {
    return !(value == null || value.isEmpty());
  }
}
