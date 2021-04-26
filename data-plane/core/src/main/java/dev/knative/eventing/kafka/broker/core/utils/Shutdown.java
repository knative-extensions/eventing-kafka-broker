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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Shutdown {

  private static final Logger logger = LoggerFactory.getLogger(Shutdown.class);

  public static Runnable run(final Vertx vertx, final AutoCloseable... closeables) {
    return () -> {
      logger.info("Executing shutdown");
      for (AutoCloseable closeable : closeables) {
        try {
          closeable.close();
        } catch (final Exception e) {
          logger.error("Failed to close", e);
        }
      }
      closeSync(vertx).run();
    };
  }

  public static Runnable closeSync(final Vertx vertx) {
    return () -> {
      logger.info("Closing Vert.x");
      final var wait = new CountDownLatch(1);
      vertx.close(ignore -> wait.countDown());
      try {
        wait.await(2, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        logger.error("Timeout waiting for vertx close", e);
      }
    };
  }
}
