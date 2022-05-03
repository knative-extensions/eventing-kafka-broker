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
package dev.knative.eventing.kafka.broker.core;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class AsyncCloseableTest {

  @Test
  public void compose(final VertxTestContext context) {
    final var x = new AtomicInteger(0);
    final var y = new AtomicInteger(0);
    final var z = new AtomicInteger(0);
    final var closeable = AsyncCloseable.compose(
      () -> {
        x.incrementAndGet();
        return Future.succeededFuture();
      },
      () -> {
        y.incrementAndGet();
        return Future.failedFuture(new RuntimeException());
      },
      () -> {
        z.incrementAndGet();
        return Future.succeededFuture();
      }
    );

    closeable.close()
      .onSuccess(v -> context.verify(() -> {
        assertThat(x.get()).isEqualTo(1);
        assertThat(y.get()).isEqualTo(1);
        assertThat(z.get()).isEqualTo(1);
        context.completeNow();
      }))
      .onFailure(context::failNow);
  }
}
