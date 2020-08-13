/*
 * Copyright 2020 The Knative Authors
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

package dev.knative.eventing.kafka.broker.core.file;

import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker1Unwrapped;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker2Unwrapped;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
import dev.knative.eventing.kafka.broker.core.file.FileWatcher.FileFormat;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

public class FileWatcherTest {

  @Test
  @Timeout(value = 5)
  public void shouldReceiveUpdatesOnUpdateJSON() throws IOException, InterruptedException {
    shouldReceiveUpdatesOnUpdate(FileFormat.JSON);
  }

  @Test
  @Timeout(value = 5)
  public void shouldReceiveUpdatesOnUpdateProtobuf() throws IOException, InterruptedException {
    shouldReceiveUpdatesOnUpdate(FileFormat.PROTOBUF);
  }

  private void shouldReceiveUpdatesOnUpdate(
      FileFormat format) throws IOException, InterruptedException {
    final var file = Files.createTempFile("fw-", "-fw").toFile();

    final var broker1 = Brokers.newBuilder()
        .addBrokers(broker1Unwrapped())
        .build();

    final var broker2 = Brokers.newBuilder()
        .addBrokers(broker2Unwrapped())
        .build();

    final var isFirst = new AtomicBoolean(true);
    final var waitFirst = new CountDownLatch(1);
    final var waitSecond = new CountDownLatch(1);
    final Consumer<Brokers> brokersConsumer = broker -> {

      if (isFirst.getAndSet(false)) {
        assertThat(broker).isEqualTo(broker1);
        waitFirst.countDown();
      } else {
        assertThat(broker).isEqualTo(broker2);
        waitSecond.countDown();
      }
    };

    final var fw = new FileWatcher(
        FileSystems.getDefault().newWatchService(),
        brokersConsumer,
        file,
        format
    );

    final var thread1 = watch(fw);
    final var thread2 = watch(fw); // the second time is no-op

    Thread.sleep(1000);

    write(file, broker1, format);
    waitFirst.await();

    write(file, broker2, format);
    waitSecond.await();

    thread1.interrupt();
    thread2.interrupt();
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "protobuf"})
  @Timeout(value = 5)
  public void shouldReadFileWhenStartWatchingWithoutUpdates(
      String f)
      throws IOException, InterruptedException {

    final var format = FileFormat.from(f);

    final var file = Files.createTempFile("fw-", "-fw").toFile();

    final var broker1 = Brokers.newBuilder()
        .addBrokers(broker1Unwrapped())
        .build();
    write(file, broker1, format);

    final var waitBroker = new CountDownLatch(1);
    final Consumer<Brokers> brokersConsumer = broker -> {
      assertThat(broker).isEqualTo(broker1);
      waitBroker.countDown();
    };

    final var fw = new FileWatcher(
        FileSystems.getDefault().newWatchService(),
        brokersConsumer,
        file,
        format
    );

    final var thread = watch(fw);

    waitBroker.await();

    thread.interrupt();
  }

  private Thread watch(FileWatcher fw) {
    final var thread = new Thread(() -> {
      try {
        fw.watch();
      } catch (InterruptedException ignored) {
      }
    });
    thread.start();
    return thread;
  }

  public static void write(final File file, final Brokers brokers, final FileFormat format) {

    switch (format) {
      case JSON -> writeJson(file, brokers);
      case PROTOBUF -> writeProtobuf(file, brokers);
    }

    LoggerFactory.getLogger(FileWatcherTest.class).info("file written");
  }

  private static void writeProtobuf(final File file, final Brokers brokers) {
    try (final var out = new FileOutputStream(file)) {
      brokers.writeTo(out);
      out.flush();
    } catch (final Exception ex) {
      fail(ex);
    }
  }

  private static void writeJson(final File file, final Brokers brokers) {
    try (final var out = new FileWriter(file)) {
      JsonFormat.printer().appendTo(brokers, out);
      out.flush();
    } catch (IOException ex) {
      fail(ex);
    }
  }
}