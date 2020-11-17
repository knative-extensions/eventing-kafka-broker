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

package dev.knative.eventing.kafka.broker.core.tracing;

import io.opentelemetry.context.propagation.TextMapPropagator;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;

final class HttpHeadersPropagatorGetter implements TextMapPropagator.Getter<Iterable<Entry<String, String>>> {

  @Override
  public Iterable<String> keys(final Iterable<Entry<String, String>> carrier) {
    return StreamSupport.stream(carrier.spliterator(), false)
      .map(Entry::getKey)
      .collect(Collectors.toSet());
  }

  @Override
  public String get(final Iterable<Entry<String, String>> carrier, @Nonnull final String key) {
    if (carrier == null) {
      return null;
    }

    return StreamSupport.stream(carrier.spliterator(), false)
      .filter(e -> e.getKey().equals(key))
      .findFirst()
      .map(Entry::getKey)
      .orElse(null);
  }
}
