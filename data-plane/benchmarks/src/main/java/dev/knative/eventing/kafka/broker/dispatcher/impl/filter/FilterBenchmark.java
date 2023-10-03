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
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.All)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public abstract class FilterBenchmark {
    Filter filter;
    CloudEvent cloudEvent;

    @Setup
    public void setupFilter() {
        this.filter = createFilter();
    }

    @Setup
    public void setupCloudEvent() {
        this.cloudEvent = createEvent();
    }

    protected abstract Filter createFilter();

    protected abstract CloudEvent createEvent();

    @Benchmark
    public void benchmarkFilterCreation(Blackhole bh) {
        bh.consume(this.createFilter());
    }

    @Benchmark
    public void benchmarkFilterEvaluation(Blackhole bh) {
        bh.consume(this.filter.test(this.cloudEvent));
    }
}
