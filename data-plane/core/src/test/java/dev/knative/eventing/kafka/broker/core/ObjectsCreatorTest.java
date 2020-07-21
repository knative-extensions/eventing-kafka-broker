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

package dev.knative.eventing.kafka.broker.core;

import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker1;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker2;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.brokers;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger1;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger2;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger3;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.trigger4;
import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.Future;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
public class ObjectsCreatorTest {

  @Test
  public void shouldPassAllTriggers() {
    final var called = new AtomicBoolean(false);

    final var brokers = Map.of(
        broker1(), Set.of(trigger1(), trigger2()),
        broker2(), Set.of(trigger3(), trigger4())
    );

    final var creator = new ObjectsCreator(objects -> {
      called.set(true);
      assertThat(objects).usingRecursiveComparison().isEqualTo(brokers);
      return Future.succeededFuture();
    });

    creator.accept(brokers());

    assertThat(called.get()).isTrue();
  }
}