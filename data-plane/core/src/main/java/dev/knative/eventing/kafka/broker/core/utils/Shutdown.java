package dev.knative.eventing.kafka.broker.core.utils;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.Contract;
import io.vertx.core.Vertx;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Shutdown {

  private static final Logger logger = LoggerFactory.getLogger(Shutdown.class);

  public static Runnable run(final Vertx vertx, final Closeable fw, final Consumer<Contract> publisher) {
    return () -> {
      try {
        fw.close();
      } catch (final IOException e) {
        logger.error("Failed to close file watcher", e);
      }
      publisher.accept(Contract.newBuilder().build());
      closeSync(vertx).run();
    };
  }

  public static Runnable closeSync(final Vertx vertx) {
    return () -> {
      final var wait = new CountDownLatch(1);
      vertx.close(ignore -> wait.countDown());
      try {
        wait.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error("Timeout waiting for vertx close", e);
      }
    };
  }
}
