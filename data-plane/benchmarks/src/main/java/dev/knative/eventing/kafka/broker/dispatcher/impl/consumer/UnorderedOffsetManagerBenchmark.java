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
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class UnorderedOffsetManagerBenchmark {

    @State(Scope.Thread)
    public static class RecordsState {

        private ConsumerRecord<String, CloudEvent>[][] records;

        @Setup(Level.Trial)
        @SuppressWarnings("unchecked")
        public void doSetup() {
            this.records = new ConsumerRecord[100][10_000];
            for (int p = 0; p < 100; p++) {
                for (int o = 0; o < 10_000; o++) {
                    this.records[p][o] = new ConsumerRecord<>("abc", p, o, null, null);
                }
            }
        }
    }

    @Benchmark
    public void benchmarkReverseOrder(RecordsState recordsState, Blackhole blackhole) {
        final OffsetManager offsetManager = new OffsetManager(Vertx.vertx(), new MockKafkaConsumer(), null, 10000L);

        int partitions = 100;
        for (int partition = 0; partition < partitions; partition++) {
            offsetManager.recordReceived(recordsState.records[partition][0]);
        }

        for (int offset = 9_999; offset > 0; offset--) {
            for (int partition = 0; partition < partitions; partition++) {
                offsetManager.recordReceived(recordsState.records[partition][offset]);
                offsetManager.successfullySentToSubscriber(recordsState.records[partition][offset]);
            }
        }

        for (int partition = 0; partition < partitions; partition++) {
            offsetManager.successfullySentToSubscriber(recordsState.records[partition][0]);
        }
    }

    @Benchmark
    public void benchmarkOrdered(RecordsState recordsState, Blackhole blackhole) {
        OffsetManager offsetManager = new OffsetManager(Vertx.vertx(), new MockKafkaConsumer(), null, 10000L);
        int partitions = 100;

        for (int offset = 0; offset < 10_000; offset++) {
            for (int partition = 0; partition < partitions; partition++) {
                offsetManager.recordReceived(recordsState.records[partition][offset]);
                offsetManager.successfullySentToSubscriber(recordsState.records[partition][offset]);
            }
        }
    }

    @Benchmark
    public void benchmarkRealisticCase(RecordsState recordsState, Blackhole blackhole) {
        OffsetManager offsetManager = new OffsetManager(Vertx.vertx(), new MockKafkaConsumer(), null, 10000L);
        int partitions = 10;

        for (int partition = 0; partition < partitions; partition++) {
            offsetManager.recordReceived(recordsState.records[partition][0]);
        }

        for (int partition = 0; partition < partitions; partition++) {
            for (int offset : new int[] {5, 2, 0, 7, 1, 3, 4, 6}) {
                offsetManager.successfullySentToSubscriber(recordsState.records[partition][offset]);
            }
        }
    }

    @Benchmark
    public void benchmarkMixedABit(RecordsState recordsState, Blackhole blackhole) {
        OffsetManager offsetManager = new OffsetManager(Vertx.vertx(), new MockKafkaConsumer(), null, 10000L);
        int partitions = 4;

        for (int partition = 0; partition < partitions; partition++) {
            offsetManager.recordReceived(recordsState.records[partition][0]);
        }

        for (int i = 0; i < 120; i++) {
            // This will commit in the following order:
            // 1 0 3 2 5 4 ...
            offsetManager.successfullySentToSubscriber(recordsState.records[2][i % 2 == 0 ? i + 1 : i - 1]);
            offsetManager.successfullySentToSubscriber(recordsState.records[1][i % 2 == 0 ? i + 1 : i - 1]);
            offsetManager.successfullySentToSubscriber(recordsState.records[0][i % 2 == 0 ? i + 1 : i - 1]);
            offsetManager.successfullySentToSubscriber(recordsState.records[3][i % 2 == 0 ? i + 1 : i - 1]);
        }
    }

    static class MockKafkaConsumer implements ReactiveKafkaConsumer<String, CloudEvent> {

        @Override
        public Future<Map<TopicPartition, OffsetAndMetadata>> commit(Map<TopicPartition, OffsetAndMetadata> offset) {

            return null;
        }

        @Override
        public Future<Void> close() {
            return null;
        }

        @Override
        public Future<Void> pause(Collection<TopicPartition> partitions) {
            return null;
        }

        @Override
        public Future<ConsumerRecords<String, CloudEvent>> poll(Duration timeout) {
            return null;
        }

        @Override
        public Future<Void> resume(Collection<TopicPartition> partitions) {
            return null;
        }

        @Override
        public Future<Void> subscribe(Collection<String> topics) {
            return null;
        }

        @Override
        public Future<Void> subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            return null;
        }

        @Override
        public Consumer<String, CloudEvent> unwrap() {
            return null;
        }

        @Override
        public ReactiveKafkaConsumer<String, CloudEvent> exceptionHandler(Handler<Throwable> handler) {
            return null;
        }
    }
}
