package dev.knative.eventing.kafka.broker.tests.core;

import static dev.knative.eventing.kafka.broker.core.file.FileWatcher.FileFormat;

import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Trigger;
import dev.knative.eventing.kafka.broker.core.file.FileWatcherTest;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import joptsimple.internal.Strings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1, warmups = 1)
public class ConfigUnmarshalling {

  private static final Brokers noBrokers = Brokers.newBuilder().build();
  private static final Brokers full;
  private static final int LONGEST_NAME = 253;
  private static final int LONGEST_BROKER_NAME = LONGEST_NAME;
  private static final int LONGEST_NAMESPACE_NAME = LONGEST_NAME;
  private static final String K8S_SERVICE_SUFFIX = "svc.cluster.local"; // usual case

  static {
    final var brokers = Brokers.newBuilder()
        .setVolumeGeneration(Long.MAX_VALUE);

    final var broker = Broker.newBuilder();
    for (int j = 0; j < 6; j++) {
      broker.addTriggers(Trigger.newBuilder()
          .putAttributes("source",
              Strings.repeat('a', LONGEST_NAMESPACE_NAME + LONGEST_BROKER_NAME) + K8S_SERVICE_SUFFIX
          )
          .putAttributes("type", Strings.repeat('a', LONGEST_NAME))
          .putAttributes("subject", Strings.repeat('b', LONGEST_NAME))
          .setDestination(
              Strings.repeat('c', LONGEST_NAMESPACE_NAME + LONGEST_BROKER_NAME) + K8S_SERVICE_SUFFIX
          )
          .setId(UUID.randomUUID().toString())
          .build()
      );
    }

    for (int i = 0; i < 100; i++) {
      brokers.addBrokers(broker.build());
    }

    full = brokers.build();
  }

  private static class BenchmarkState {

    private final File file;

    private BenchmarkState(final Brokers brokers, final FileFormat format) {
      try {
        file = Files.createTempFile("fw-", "-fw").toFile();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      FileWatcherTest.write(file, brokers, format);
    }

    public File getFile() {
      return file;
    }
  }

  @State(Scope.Thread)
  public static class BenchmarkStateJsonNoBrokers {

    File file;

    /**
     * Create the state for Json benchmark.
     */
    public BenchmarkStateJsonNoBrokers() {
      file = new BenchmarkState(noBrokers, FileFormat.JSON).getFile();
    }
  }

  @State(Scope.Thread)
  public static class BenchmarkStateJsonFull {

    File file;

    /**
     * Create the state for Json benchmark.
     */
    public BenchmarkStateJsonFull() {
      file = new BenchmarkState(full, FileFormat.JSON).getFile();
    }
  }

  @State(Scope.Thread)
  public static class BenchmarkStateProtoNoBrokers {

    final File file;

    /**
     * Create the state for Proto benchmark.
     */
    public BenchmarkStateProtoNoBrokers() {
      file = new BenchmarkState(noBrokers, FileFormat.PROTOBUF).getFile();
    }
  }

  @State(Scope.Thread)
  public static class BenchmarkStateProtoFull {

    final File file;

    /**
     * Create the state for Proto benchmark.
     */
    public BenchmarkStateProtoFull() {
      file = new BenchmarkState(full, FileFormat.PROTOBUF).getFile();
    }
  }

  /**
   * Benchmark Json unmarshalling.
   *
   * @param state benchmark state.
   * @return brokers parsed
   * @throws IOException if something goes wrong.
   */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public Brokers benchmarkJsonUnmarshallingNoBrokers(final BenchmarkStateJsonFull state)
      throws IOException {
    return jsonUnmarshalling(state.file);
  }

  /**
   * Benchmark Json unmarshalling.
   *
   * @param state benchmark state.
   * @return brokers parsed
   * @throws IOException if something goes wrong.
   */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public Brokers benchmarkJsonUnmarshallingFull(final BenchmarkStateJsonNoBrokers state)
      throws IOException {
    return jsonUnmarshalling(state.file);
  }

  private Brokers jsonUnmarshalling(final File file) throws IOException {
    try (
        final var fileReader = new FileReader(file);
        final var bufferedReader = new BufferedReader(fileReader)) {

      final var brokers = Brokers.newBuilder();
      JsonFormat.parser().merge(bufferedReader, brokers);
      return brokers.build();
    }
  }

  /**
   * Benchmark Proto unmarshalling.
   *
   * @param state benchmark state.
   * @return brokers parsed
   * @throws IOException if something goes wrong.
   */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public Brokers benchmarkProtoUnmarshallingNoBrokers(final BenchmarkStateProtoNoBrokers state)
      throws IOException {
    return protoUnmarshalling(state.file);
  }

  /**
   * Benchmark Proto unmarshalling.
   *
   * @param state benchmark state.
   * @return brokers parsed
   * @throws IOException if something goes wrong.
   */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public Brokers benchmarkProtoUnmarshallingFull(final BenchmarkStateProtoFull state)
      throws IOException {
    return protoUnmarshalling(state.file);
  }

  private static Brokers protoUnmarshalling(final File file) throws IOException {
    try (final var in = new FileInputStream(file);
        final var bufferedInputStream = new BufferedInputStream(in)) {

      final var bytes = bufferedInputStream.readAllBytes();
      if (bytes.length == 0) {
        return null;
      }

      return Brokers.parseFrom(bytes);
    }
  }

  /**
   * Run this benchmark.
   *
   * @param args command line arguments.
   */
  public static void main(String[] args) throws IOException {
    org.openjdk.jmh.Main.main(args);
  }
}
