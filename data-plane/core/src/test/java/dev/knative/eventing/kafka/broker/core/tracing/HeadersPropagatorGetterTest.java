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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class HeadersPropagatorGetterTest {

  @Test
  public void shouldGetAllKeys() {

    final var getter = new HeadersPropagatorGetter();

    final var keys = getter.keys(Arrays.asList(
      new SimpleImmutableEntry<>("a", "1"),
      new SimpleImmutableEntry<>("b", "1")
    ));

    assertThat(keys).containsAll(Arrays.asList("a", "b"));
  }

  @Test
  public void shouldReturnNullWhenThereIsNotKeyInCarrier() {

    final var getter = new HeadersPropagatorGetter();

    final Iterable<Map.Entry<String, String>> carrier = Arrays.asList(
      new SimpleImmutableEntry<>("a", "1"),
      new SimpleImmutableEntry<>("b", "1")
    );

    final var get = getter.get(carrier, "c");

    assertThat(get).isNull();
  }

  @Test
  public void shouldReturnValueWhenThereIsAKeyInCarrierCaseInsensitive() {

    final var getter = new HeadersPropagatorGetter();

    final Iterable<Map.Entry<String, String>> carrier = Arrays.asList(
      new SimpleImmutableEntry<>("a", "1"),
      new SimpleImmutableEntry<>("b", "2")
    );

    final var get = getter.get(carrier, "A");

    assertThat(get).isEqualTo("1");
  }

  @Test
  public void shouldReturnNullWhenCarrierIsNull() {

    final var getter = new HeadersPropagatorGetter();

    final var get = getter.get(null, "A");

    assertThat(get).isNull();
  }
}
