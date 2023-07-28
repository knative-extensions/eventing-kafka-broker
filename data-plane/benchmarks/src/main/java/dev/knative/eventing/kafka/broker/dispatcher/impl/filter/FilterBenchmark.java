package dev.knative.eventing.kafka.broker.dispatcher.impl.filter;

import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import io.cloudevents.CloudEvent;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

public abstract class FilterBenchmark{
  @State(Scope.Thread)
  public static class FilterEvaluationState {
    Filter filter;
    CloudEvent event;

    @Setup
    public void setupFilter() {
      this.filter = createFilter();
    }

    @Setup
    public void setupEvent() {
      this.event = createEvent();
    }
  }

  protected static Filter createFilter() {
    return null;
  }

  protected static CloudEvent createEvent() {
    return null;
  }

  @Benchmark
  public void benchmarkFilterCreation(Blackhole bh) {
    bh.consume(createFilter());
  }

  @Benchmark
  @Warmup(iterations = 3)
  @Measurement(iterations = 100)
  public void benchmarkFilterEvaluation(Blackhole bh, FilterEvaluationState fes) {
    bh.consume(fes.filter.test(fes.event));
  }
}
