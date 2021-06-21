package dev.knative.eventing.kafka.broker.core;

import io.vertx.core.Closeable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
    return () -> CompositeFuture
      .all(Arrays.stream(closeables).map(AsyncCloseable::close).collect(Collectors.toList()))
      .mapEmpty();
  }

}
