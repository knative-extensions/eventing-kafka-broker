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

import io.vertx.core.Closeable;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * Interface for components that can be closed asynchronously.
 */
@FunctionalInterface
public interface AsyncCloseable extends Closeable {

  /**
   * Close this object.
   *
   * @return a future notifying the completion of the close operation
   */
  Future<Void> close();

  @Override
  default void close(Promise<Void> completion) {
    this.close().onComplete(completion);
  }

  /**
   * Transform an {@link AsyncCloseable} into a blocking {@link AutoCloseable}
   *
   * @param closeable the closeable to convert
   * @return an implementation of {@link AutoCloseable} that will block when invoked.
   */
  static AutoCloseable toAutoCloseable(AsyncCloseable closeable) {
    return () -> closeable.close().toCompletionStage().toCompletableFuture();
  }

  /**
   * Compose several {@link AsyncCloseable}s into a single {@link AsyncCloseable}.
   * One close failure will cause the whole close to fail.
   * <p>
   * It filters null futures returned by individual {@link AsyncCloseable} on close.
   *
   * @param closeables the closeables to compose
   * @return the composed closeables
   */
  static AsyncCloseable compose(AsyncCloseable... closeables) {
    return () -> compose(Arrays.stream(closeables).filter(Objects::nonNull).iterator());
  }

  private static Future<Void> compose(final Iterator<AsyncCloseable> closeableIterator) {
    if (!closeableIterator.hasNext()) {
      return Future.succeededFuture();
    }

    try {
      return closeableIterator.
        next().
        close().
        compose(
          v -> compose(closeableIterator),
          cause -> {
            LoggerFactory.getLogger(AsyncCloseable.class).warn("Failed to close closeable", cause);
            return compose(closeableIterator);
          }
        );
    } catch (final Exception ex) {
      return compose(closeableIterator);
    }
  }
}
