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

package dev.knative.eventing.kafka.broker.dispatcher.impl.filter;

import dev.knative.eventing.kafka.broker.dispatcher.Filter;
import io.cloudevents.CloudEvent;
import io.vertx.core.Vertx;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@Fork(1)
@State(Scope.Thread)
@Measurement(iterations = 3, time = 10)
@Warmup(iterations = 3, time = 5)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public abstract class FilterBenchmark {
    Filter filter;
    CloudEvent cloudEvent;

    Vertx vertx;

    public static final long FILTER_REORDER_TIME_MILLISECONDS = 1000; // 1 seconds

    @TearDown
    public void closeVertx() {
        this.vertx.close();
    }

    @Setup(Level.Trial)
    public void setupFilter() {
        this.vertx = Vertx.vertx();
        this.filter = createFilter();
    }

    @Setup(Level.Trial)
    public void setupCloudEvent() {
        this.cloudEvent = createEvent();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        this.filter.close(vertx);
    }

    protected abstract Filter createFilter();

    protected abstract CloudEvent createEvent();

    @Benchmark
    public void benchmarkFilterCreation(Blackhole bh) {
        final var filter = this.createFilter();
        filter.close(vertx);
        bh.consume(filter);
    }

    @Benchmark
    public void benchmarkFilterEvaluation(Blackhole bh) {
        bh.consume(this.filter.test(this.cloudEvent));
    }
}
