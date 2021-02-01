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
package dev.knative.eventing.kafka.broker.core.file;

import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource1;
import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.resource2;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

public class FileWatcherTest {

  @Test
  @Timeout(value = 5)
  public void shouldReceiveUpdatesOnUpdate() throws IOException, InterruptedException {
    final var file = Files.createTempFile("fw-", "-fw").toFile();

    final var broker1 = DataPlaneContract.Contract.newBuilder()
      .addResources(resource1())
      .build();

    final var broker2 = DataPlaneContract.Contract.newBuilder()
      .addResources(resource2())
      .build();

    final var isFirst = new AtomicBoolean(true);
    final var waitFirst = new CountDownLatch(1);
    final var waitSecond = new CountDownLatch(1);
    final Consumer<DataPlaneContract.Contract> brokersConsumer = broker -> {

      if (isFirst.getAndSet(false)) {
        assertThat(broker).isEqualTo(broker1);
        waitFirst.countDown();
      } else if (!broker.equals(broker1)) {
        assertThat(broker).isEqualTo(broker2);
        waitSecond.countDown();
      }
    };

    final var fw = new FileWatcher(
      FileSystems.getDefault().newWatchService(),
      brokersConsumer,
      file
    );

    final var thread1 = watch(fw);
    final var thread2 = watch(fw); // the second time is no-op

    write(file, broker1);
    waitFirst.await();

    write(file, broker2);
    waitSecond.await();

    thread1.interrupt();
    thread2.interrupt();
  }

  @Test
  @Timeout(value = 5)
  public void shouldReadFileWhenStartWatchingWithoutUpdates()
    throws IOException, InterruptedException {

    final var file = Files.createTempFile("fw-", "-fw").toFile();

    final var broker1 = DataPlaneContract.Contract.newBuilder()
      .addResources(resource1())
      .build();
    write(file, broker1);

    final var waitBroker = new CountDownLatch(1);
    final Consumer<DataPlaneContract.Contract> brokersConsumer = broker -> {
      assertThat(broker).isEqualTo(broker1);
      waitBroker.countDown();
    };

    final var fw = new FileWatcher(
      FileSystems.getDefault().newWatchService(),
      brokersConsumer,
      file
    );

    final var thread = watch(fw);

    waitBroker.await();

    thread.interrupt();
  }

  private Thread watch(FileWatcher fw) {
    final var thread = new Thread(() -> {
      try {
        fw.watch();
      } catch (IOException | InterruptedException ignored) {
      }
    });
    thread.start();
    return thread;
  }

  public static void write(File file, DataPlaneContract.Contract contract) throws IOException {
    final var f = new File(file.toString());
    try (final var out = new FileWriter(f)) {
      JsonFormat.printer().appendTo(contract, out);
    } finally {
      LoggerFactory.getLogger(FileWatcherTest.class).info("file written");
    }
  }
}
