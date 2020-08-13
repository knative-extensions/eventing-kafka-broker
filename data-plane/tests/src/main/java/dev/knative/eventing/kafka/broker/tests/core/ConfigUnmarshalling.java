package dev.knative.eventing.kafka.broker.tests.core;

import static dev.knative.eventing.kafka.broker.core.file.FileWatcher.FileFormat;

import com.google.protobuf.util.JsonFormat;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Brokers;
import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Trigger;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import joptsimple.internal.Strings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

// run benchmarks: java -jar tests/target/tests-1.0-SNAPSHOT.jar -rf csv -prof gc

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Thread)
public class ConfigUnmarshalling {

  private static final int LONGEST_NAME = 253;
  private static final int LONGEST_BROKER_NAME = LONGEST_NAME;
  private static final int LONGEST_NAMESPACE_NAME = LONGEST_NAME;
  private static final String K8S_SERVICE_SUFFIX = "svc.cluster.local"; // usual case

  private static Brokers noBrokers() {
    return Brokers.newBuilder().build();
  }

  private static Brokers full() {
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

    return brokers.build();
  }

  private static class BenchmarkState {

    final byte[] buf;

    private BenchmarkState(final Brokers brokers, final FileFormat format) {
      try {

        if (FileFormat.PROTOBUF.equals(format)) {

          final var out = new ByteArrayOutputStream();
          brokers.writeTo(out);
          buf = out.toByteArray();

        } else if (FileFormat.JSON.equals(format)) {

          buf = JsonFormat.printer().print(brokers).getBytes();

        } else {
          throw new IllegalStateException("unknown file format");
        }

      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @State(Scope.Thread)
  public static class BenchmarkStateJsonNoBrokers {

    final BenchmarkState state;

    /**
     * Create the state for Json benchmark.
     */
    public BenchmarkStateJsonNoBrokers() {
      state = new BenchmarkState(noBrokers(), FileFormat.JSON);
    }
  }

  @State(Scope.Thread)
  public static class BenchmarkStateJsonFull {

    final BenchmarkState state;

    /**
     * Create the state for Json benchmark.
     */
    public BenchmarkStateJsonFull() {
      state = new BenchmarkState(full(), FileFormat.JSON);
    }
  }

  @State(Scope.Thread)
  public static class BenchmarkStateProtoNoBrokers {

    final BenchmarkState state;

    /**
     * Create the state for Proto benchmark.
     */
    public BenchmarkStateProtoNoBrokers() {
      state = new BenchmarkState(noBrokers(), FileFormat.PROTOBUF);
    }
  }

  @State(Scope.Thread)
  public static class BenchmarkStateProtoFull {

    final BenchmarkState state;

    /**
     * Create the state for Proto benchmark.
     */
    public BenchmarkStateProtoFull() {
      state = new BenchmarkState(full(), FileFormat.PROTOBUF);
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
  public Brokers benchmarkProtoUnmarshallingNoBrokers(final BenchmarkStateProtoNoBrokers state)
      throws IOException {
    return protoUnmarshalling(state.state.buf);
  }

  /**
   * Benchmark Proto unmarshalling.
   *
   * @param state benchmark state.
   * @return brokers parsed
   * @throws IOException if something goes wrong.
   */
  @Benchmark
  public Brokers benchmarkProtoUnmarshallingFull(final BenchmarkStateProtoFull state)
      throws IOException {
    return protoUnmarshalling(state.state.buf);
  }

  private static Brokers protoUnmarshalling(final byte[] buf) throws IOException {

    final var bufferedInputStream = new BufferedInputStream(new ByteArrayInputStream(buf));

    if (bufferedInputStream.available() <= 0) {
      return Brokers.newBuilder().build();
    }

    return Brokers.parseFrom(bufferedInputStream);
  }

  /**
   * Benchmark Json unmarshalling.
   *
   * @param state benchmark state.
   * @return brokers parsed
   * @throws IOException if something goes wrong.
   */
  @Benchmark
  public Brokers benchmarkJsonUnmarshallingNoBrokers(final BenchmarkStateJsonFull state)
      throws IOException {
    return jsonUnmarshalling(state.state.buf);
  }

  /**
   * Benchmark Json unmarshalling.
   *
   * @param state benchmark state.
   * @return brokers parsed
   * @throws IOException if something goes wrong.
   */
  @Benchmark
  public Brokers benchmarkJsonUnmarshallingFull(final BenchmarkStateJsonNoBrokers state)
      throws IOException {
    return jsonUnmarshalling(state.state.buf);
  }

  private Brokers jsonUnmarshalling(final byte[] buf) throws IOException {

    final var reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buf)));

    final var brokers = Brokers.newBuilder();
    JsonFormat.parser().merge(reader, brokers);
    return brokers.build();
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
