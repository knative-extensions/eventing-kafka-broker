package dev.knative.eventing.kafka.broker.core;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class OrderedAsyncExecutorTest {

  private static Stream<Arguments> inputArgs() {
    return Stream.of(
      Arguments.of(0L, 10),
      Arguments.of(0L, 100),
      Arguments.of(0L, 1000),
      Arguments.of(0L, 10000),
      Arguments.of(50L, 500),
      Arguments.of(100L, 10),
      Arguments.of(100L, 100),
      Arguments.of(1000L, 10)
    );
  }

  @Timeout(value = 20, timeUnit = TimeUnit.SECONDS) // Longest takes 10 secs
  @ParameterizedTest
  @MethodSource("inputArgs")
  public void shouldExecuteInOrder(final long millis, final int tasks, Vertx vertx) throws InterruptedException {
    Random random = new Random();

    // Deploy the verticle
    AVerticle verticle = new AVerticle();
    CountDownLatch startVerticleLatch = new CountDownLatch(1);
    vertx.deployVerticle(verticle, v -> startVerticleLatch.countDown());
    startVerticleLatch.await();

    // Rewrite the vertx instance in order to make sure we run always in the same context
    vertx = verticle.getVertx();

    CountDownLatch tasksLatch = new CountDownLatch(tasks);
    List<Integer> executed = new ArrayList<>(tasks);

    OrderedAsyncExecutor asyncExecutor = new OrderedAsyncExecutor();

    for (int i = 0; i < tasks; i++) {
      Supplier<Future<?>> task = generateTask(vertx, random, millis, i, tasksLatch, executed);
      vertx.runOnContext((v) -> asyncExecutor.offer(task));
    }

    tasksLatch.await();

    // Check if tasks were executed in order
    IntStream.range(0, tasks)
      .forEach(i -> {
        int actual = executed.get(i);
        assertThat(actual).isEqualTo(i);
      });
  }

  @Test
  public void shouldStop(Vertx vertx) throws InterruptedException {
    int tasks = 10;
    Random random = new Random();

    // Deploy the verticle
    AVerticle verticle = new AVerticle();
    CountDownLatch startVerticleLatch = new CountDownLatch(1);
    vertx.deployVerticle(verticle, v -> startVerticleLatch.countDown());
    startVerticleLatch.await();

    // Rewrite the vertx instance in order to make sure we run always in the same context
    vertx = verticle.getVertx();

    List<Integer> executed = new ArrayList<>(tasks);

    OrderedAsyncExecutor asyncExecutor = new OrderedAsyncExecutor();

    for (int i = 0; i < tasks; i++) {
      Supplier<Future<?>> task = generateTask(vertx, random, 100, i, null, executed);
      vertx.runOnContext((v) -> asyncExecutor.offer(task));
    }

    asyncExecutor.stop();

    // Let's wait for a bunch of seconds before asserting
    // We don't need to wait for any event to happen, we just want to make sure
    // that the async executor actually stopped and no other task was executed
    Thread.sleep(1000);

    assertThat(executed).hasSizeLessThan(tasks);
  }

  private Supplier<Future<?>> generateTask(Vertx vertx, Random random,
                                           long millis, int i,
                                           CountDownLatch latch, List<Integer> executed) {
    if (millis == 0) {
      return () -> {
        if (latch != null) {
          latch.countDown();
        }
        executed.add(i);
        return Future.succeededFuture();
      };
    } else {
      // Some random number around the provided millis
      long delay = Math.round(millis + ((random.nextDouble() - 0.5) * millis * 0.5));

      return () -> {
        Promise<Void> prom = Promise.promise();
        vertx.setTimer(delay, v -> {
          if (latch != null) {
            latch.countDown();
          }
          executed.add(i);
          prom.complete();
        });
        return prom.future();
      };
    }
  }

  private class AVerticle extends AbstractVerticle {
  }

}
