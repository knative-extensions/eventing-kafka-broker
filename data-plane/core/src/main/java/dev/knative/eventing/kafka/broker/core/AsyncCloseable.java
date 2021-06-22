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
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.Arrays;
import java.util.stream.Collectors;

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
   * @return an implementation of {@link AutoCloseable} that will block when invoked.
   */
  default AutoCloseable toAutoCloseable() {
    return () -> this.close().toCompletionStage().toCompletableFuture();
  }

  /**
   * Compose several {@link AsyncCloseable} into a single {@link AsyncCloseable}. One close failure will cause the whole close to fail.
   *
   * @param closeables the closeables to compose
   * @return the composed closeables
   */
  static AsyncCloseable compose(AsyncCloseable... closeables) {
    return () -> CompositeFuture.all(
      Arrays.stream(closeables)
        .map(AsyncCloseable::close)
        .collect(Collectors.toList())
    ).mapEmpty();
  }

  /**
   * Wrap the provided blocking {@link AutoCloseable} into an {@link AsyncCloseable}.
   *
   * @param context the context to use to execute the blocking operation
   * @param closeable the closeable to wrap
   * @return the wrapped closeable
   */
  static AsyncCloseable wrapAutoCloseable(Context context, AutoCloseable closeable) {
    return () -> context.executeBlocking(promise -> {
      try {
        closeable.close();
        promise.complete();
      } catch (Exception e) {
        promise.fail(e);
      }
    });
  }

  /**
   * Like {@link #wrapAutoCloseable(Context, AutoCloseable)} but using the current context, if any, when the close is invoked.
   */
  static AsyncCloseable wrapAutoCloseable(AutoCloseable closeable) {
    return () -> Vertx.currentContext().executeBlocking(promise -> {
      try {
        closeable.close();
        promise.complete();
      } catch (Exception e) {
        promise.fail(e);
      }
    });
  }

}
