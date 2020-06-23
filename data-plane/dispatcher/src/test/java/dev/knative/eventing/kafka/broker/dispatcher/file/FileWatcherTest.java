package dev.knative.eventing.kafka.broker.dispatcher.file;

import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker1Unwrapped;
import static dev.knative.eventing.kafka.broker.core.testing.utils.CoreObjects.broker2Unwrapped;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
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

    final var broker1 = Brokers.newBuilder()
        .addBroker(broker1Unwrapped())
        .build();

    final var broker2 = Brokers.newBuilder()
        .addBroker(broker2Unwrapped())
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
        file
    );

    final var thread1 = watch(fw);
    final var thread2 = watch(fw); // the second time is no-op

    Thread.sleep(1000);

    write(file, broker1);
    waitFirst.await();

    write(file, broker2);
    waitSecond.await();

    thread1.interrupt();
    thread2.interrupt();
  }

  private Thread watch(FileWatcher fw) {
    final var thread = new Thread(() -> {
      try {
        fw.watch();
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    });
    thread.start();
    return thread;
  }

  public static void write(File file, Brokers brokers) throws IOException {
    final var f = new File(file.toString());
    try (final var out = new FileWriter(f)) {
      JsonFormat.printer().appendTo(brokers, out);
    } finally {
      LoggerFactory.getLogger(FileWatcherTest.class).info("file written");
    }
  }
}