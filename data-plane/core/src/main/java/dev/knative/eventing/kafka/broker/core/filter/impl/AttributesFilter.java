/*
 * Copyright 2020 The Knative Authors
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
package dev.knative.eventing.kafka.broker.core.filter.impl;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v03.CloudEventV03;
import io.cloudevents.core.v1.CloudEventV1;
import io.cloudevents.lang.Nullable;
import java.net.URI;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AttributesFilter implements Filter {

  private static final String DEFAULT_STRING = "";

  static final Map<String, Function<CloudEvent, String>> ATTRIBUTES_MAPPER =
      Map.of(
          CloudEventV1.SPECVERSION, event -> event.getSpecVersion().toString(),
          CloudEventV1.ID, CloudEvent::getId,
          CloudEventV1.TYPE, CloudEvent::getType,
          CloudEventV1.SOURCE, event -> event.getSource().toString(),
          CloudEventV1.DATACONTENTTYPE, CloudEvent::getDataContentType,
          CloudEventV1.DATASCHEMA, event -> getOrDefault(event.getDataSchema(), URI::toString),
          CloudEventV03.SCHEMAURL, event -> getOrDefault(event.getDataSchema(), URI::toString),
          CloudEventV1.SUBJECT, CloudEvent::getSubject,
          CloudEventV1.TIME,
              event -> getOrDefault(event.getTime(), time -> time.format(ISO_INSTANT)));

  // the key represents the function to turn an event into a string value.
  // the value represents the value to match.
  // specversion -> 1.0
  // f(event) -> event.getSpecVersion().toString() -> 1.0
  private final List<Entry<Function<CloudEvent, String>, String>> attributes;

  /**
   * All args constructor.
   *
   * @param attributes attributes to match to pass filter.
   */
  public AttributesFilter(final Map<String, String> attributes) {
    this.attributes =
        attributes.entrySet().stream()
            .filter(entry -> isNotEmpty(entry.getValue()))
            .map(
                entry ->
                    new SimpleImmutableEntry<>(
                        ATTRIBUTES_MAPPER.getOrDefault(
                            entry.getKey(),
                            event -> {
                              try {
                                return getOrDefault(
                                    event.getAttribute(entry.getKey()), Object::toString);
                              } catch (final Exception ex) {
                                return getOrDefault(
                                    event.getExtension(entry.getKey()), Object::toString);
                              }
                            }),
                        entry.getValue()))
            .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Attributes filters events by exact match on event context attributes. Each key in the map is
   * compared with the equivalent key in the event context. An event passes the filter if all values
   * are equal to the specified values. Nested context attributes are not supported as keys. Only
   * string values are supported.
   *
   * @param event event to match
   * @return true if event matches attributes, otherwise false.
   */
  @Override
  public boolean test(final CloudEvent event) {

    for (final var entry : attributes) {
      if (!entry.getKey().apply(event).equals(entry.getValue())) {
        return false;
      }
    }

    return true;
  }

  private static <T> String getOrDefault(
      @Nullable final T s, final Function<T, String> stringProvider) {

    if (s == null) {
      return DEFAULT_STRING;
    }
    return stringProvider.apply(s);
  }

  private static boolean isNotEmpty(final String value) {
    return !(value == null || value.isEmpty());
  }
}
