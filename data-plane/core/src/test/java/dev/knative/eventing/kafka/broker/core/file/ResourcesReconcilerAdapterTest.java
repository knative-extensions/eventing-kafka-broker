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

package dev.knative.eventing.kafka.broker.core.file;

import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.contract;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress1;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress2;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress3;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.egress4;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource1;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource2;
import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.Future;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
public class ResourcesReconcilerAdapterTest {

  @Test
  public void shouldPassAllEgresses() {
    final var called = new AtomicBoolean(false);

    final var resources = Map.of(
      resource1(), Set.of(egress1(), egress2()),
      resource2(), Set.of(egress3(), egress4())
    );

    final var creator = new ResourcesReconcilerAdapter(objects -> {
      called.set(true);
      assertThat(objects).usingRecursiveComparison().isEqualTo(resources);
      return Future.succeededFuture();
    });

    creator.accept(contract());

    assertThat(called.get()).isTrue();
  }
}
