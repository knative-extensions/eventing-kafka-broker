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
package dev.knative.eventing.kafka.broker.core.utils;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.io.Closeable;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class ShutdownTest {

    @Test
    public void run() throws IOException {
        final var vertx = mockVertxClose();
        final var closeable = mock(Closeable.class);

        Shutdown.createRunnable(vertx, closeable).run();

        verify(vertx, times(1)).close();
        verify(closeable).close();
    }

    @Test
    public void closeVertxSync() {
        final var vertx = mockVertxClose();

        Shutdown.closeVertxSync(vertx);

        verify(vertx).close();
    }

    private Vertx mockVertxClose() {
        final var vertx = mock(Vertx.class);
        doReturn(Future.succeededFuture()).when(vertx).close();
        return vertx;
    }
}
