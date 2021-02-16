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
package dev.knative.eventing.kafka.broker.core.tracing;

import io.opentelemetry.context.propagation.TextMapPropagator;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HeadersPropagatorGetter implements TextMapPropagator.Getter<Iterable<Entry<String, String>>> {

  private static final Logger logger = LoggerFactory.getLogger(HeadersPropagatorGetter.class);

  @Override
  public Iterable<String> keys(final Iterable<Entry<String, String>> carrier) {
    final var keys = StreamSupport.stream(carrier.spliterator(), false)
      .map(Entry::getKey)
      .collect(Collectors.toSet());

    return keys;
  }

  @Override
  public String get(final Iterable<Entry<String, String>> carrier, @Nonnull final String key) {
    if (carrier == null) {
      return null;
    }

    final var value = StreamSupport.stream(carrier.spliterator(), false)
      .filter(e -> e.getKey().equalsIgnoreCase(key))
      .findFirst()
      .map(Entry::getValue)
      .orElse(null);

    return value;
  }
}
