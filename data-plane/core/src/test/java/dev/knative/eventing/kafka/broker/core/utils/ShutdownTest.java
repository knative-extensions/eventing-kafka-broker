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

    doAnswer(invocation -> {
      final Handler<AsyncResult<Void>> callback = invocation.getArgument(0);
      callback.handle(Future.succeededFuture());
      return null;
    })
      .when(vertx).close(any());

    return vertx;
  }
}
