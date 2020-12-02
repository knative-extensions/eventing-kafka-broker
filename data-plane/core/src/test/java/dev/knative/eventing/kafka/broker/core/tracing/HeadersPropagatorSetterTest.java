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
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;

public class HeadersPropagatorSetterTest {

  @Test
  public void shouldCallConsumerWhenCarrierIsNotNull() {

    final var nCalls = new AtomicInteger(0);
    final BiConsumer<String, String> c = (k, v) -> {
      nCalls.incrementAndGet();

      assertThat(k).isEqualTo("k");
      assertThat(v).isEqualTo("v");
    };

    final var setter = new HeadersPropagatorSetter();

    setter.set(c, "k", "v");

    assertThat(nCalls.get()).isEqualTo(1);
  }

  @Test
  public void shouldNotThrowWhenCarrierIsNull() {
    assertThatNoException().isThrownBy(() -> new HeadersPropagatorSetter().set(null, "k", "v"));
  }
}
