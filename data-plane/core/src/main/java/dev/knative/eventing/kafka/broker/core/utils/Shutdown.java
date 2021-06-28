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

import io.vertx.core.Vertx;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Shutdown {

  private static final Logger logger = LoggerFactory.getLogger(Shutdown.class);

  private Shutdown() {
  }

  /**
   * Register a set of {@link AutoCloseable} in the runtime shutdown hook.
   * This is going to shutdown the set of provided autocloseable, and then the vertx instance.
   *
   * @param vertx      the vertx instance to shutdown
   * @param closeables the set of auto closeable to shutdown
   */
  public static void registerHook(final Vertx vertx, final AutoCloseable... closeables) {
    Runtime.getRuntime()
      .addShutdownHook(new Thread(
        Shutdown.createRunnable(vertx, closeables)
      ));
  }

  /**
   * Force closing the provided {@link Vertx} instance synchronously.
   * This method is infallible and will log any eventual error.
   *
   * @param vertx the vertx instance to close
   */
  public static void closeVertxSync(final Vertx vertx) {
    logger.info("Closing Vert.x");
    try {
      vertx.close()
        .toCompletionStage()
        .toCompletableFuture()
        .get(2, TimeUnit.MINUTES);
    } catch (TimeoutException e) {
      logger.error("Timeout waiting for Vertx::close", e);
    } catch (Throwable e) {
      logger.error("Error while closing Vertx instance", e);
    }
  }

  static Runnable createRunnable(final Vertx vertx, final AutoCloseable... closeables) {
    return () -> {
      logger.info("Running shutdown hook");
      for (AutoCloseable closeable : closeables) {
        try {
          closeable.close();
        } catch (final Exception e) {
          logger.error("Failed to close", e);
        }
      }
      closeVertxSync(vertx);
    };
  }
}
