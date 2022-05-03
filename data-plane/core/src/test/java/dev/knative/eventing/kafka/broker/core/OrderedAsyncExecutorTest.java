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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
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
      Arguments.of(50L, 50),
      Arguments.of(50L, 50),
      Arguments.of(50L, 250),
      Arguments.of(100L, 10),
      Arguments.of(100L, 100),
      Arguments.of(1000L, 10)
    );
  }

  @ParameterizedTest(name = "with delay {0}ms and tasks {1}")
  @MethodSource("inputArgs")
  public void shouldExecuteInOrder(final long millis, final int tasks, final Vertx parentVertx) throws InterruptedException {
    Random random = new Random();

    // Deploy the verticle
    AVerticle verticle = new AVerticle();
    CountDownLatch startVerticleLatch = new CountDownLatch(1);
    parentVertx.deployVerticle(verticle, v -> startVerticleLatch.countDown());
    startVerticleLatch.await();

    // Rewrite the vertx instance in order to make sure we run always in the same context
    final var vertx = verticle.getVertx();

    CountDownLatch tasksLatch = new CountDownLatch(tasks);
    List<Integer> executed = new ArrayList<>(tasks);

    OrderedAsyncExecutor asyncExecutor = new OrderedAsyncExecutor();

    for (int i = 0; i < tasks; i++) {
      final var n = i;
      vertx.runOnContext((v) -> asyncExecutor.offer(generateTask(vertx, random, millis, n, tasksLatch, executed)));
    }

    assertThat(
      tasksLatch.await(20, TimeUnit.SECONDS) // Longest takes 10 secs
    ).isTrue();

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

    CountDownLatch tasksLatch = new CountDownLatch(tasks);
    OrderedAsyncExecutor asyncExecutor = new OrderedAsyncExecutor();

    for (int i = 0; i < tasks; i++) {
      Supplier<Future<?>> task = generateTask(vertx, random, 100, i, tasksLatch, executed);
      vertx.runOnContext((v) -> asyncExecutor.offer(task));
    }

    asyncExecutor.stop();

    // Let's wait for a bunch of seconds before asserting
    // We don't need to wait for any event to happen, we just want to make sure
    // that the async executor actually stopped and no other task was executed
    // If this await returns false, then the latch didn't count down to 0
    assertThat(
      tasksLatch.await(1, TimeUnit.SECONDS)
    ).isFalse();

    assertThat(executed).hasSizeLessThan(tasks);
  }

  private Supplier<Future<?>> generateTask(Vertx vertx, Random random,
                                           long millis, int i,
                                           CountDownLatch latch, List<Integer> executed) {
    if (millis == 0) {
      return () -> {
        executed.add(i);
        latch.countDown();
        return Future.succeededFuture();
      };
    } else {
      // Some random number around the provided millis
      long delay = Math.round(millis + ((random.nextDouble() - 0.5) * millis * 0.5));

      return () -> {
        Promise<Void> prom = Promise.promise();
        vertx.setTimer(delay, v -> {
          executed.add(i);
          latch.countDown();
          prom.complete();
        });
        return prom.future();
      };
    }
  }

  private static class AVerticle extends AbstractVerticle {
  }

}
