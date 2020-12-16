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
package dev.knative.eventing.kafka.broker.core.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Contract;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

public class ShutdownTest {

  @Test
  public void run() throws IOException {
    final var vertx = mockVertxClose();
    final var closeable = mock(Closeable.class);
    final Consumer<Contract> consumer = mock(Consumer.class);

    Shutdown.run(vertx, closeable, consumer).run();

    verify(vertx, times(1)).close(any());
    verify(closeable).close();
    verify(consumer).accept(Contract.newBuilder().build());
  }

  @Test
  public void closeSync() {
    final var vertx = mockVertxClose();

    Shutdown.closeSync(vertx).run();

    verify(vertx).close(any());
  }

  private Vertx mockVertxClose() {
    final var vertx = mock(Vertx.class);

    doAnswer(
            invocation -> {
              final Handler<AsyncResult<Void>> callback = invocation.getArgument(0);
              callback.handle(Future.succeededFuture());
              return null;
            })
        .when(vertx)
        .close(any());

    return vertx;
  }
}
