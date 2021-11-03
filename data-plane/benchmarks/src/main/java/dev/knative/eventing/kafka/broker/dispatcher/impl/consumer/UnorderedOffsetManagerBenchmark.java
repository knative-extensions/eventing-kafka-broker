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

import io.cloudevents.CloudEvent;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.consumer.OffsetAndTimestamp;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class UnorderedOffsetManagerBenchmark {

  @State(Scope.Thread)
  public static class RecordsState {

    private KafkaConsumerRecord<String, CloudEvent>[][] records;

    @Setup(Level.Trial)
    @SuppressWarnings("unchecked")
    public void doSetup() {
      this.records = new KafkaConsumerRecord[100][10_000];
      for (int p = 0; p < 100; p++) {
        for (int o = 0; o < 10_000; o++) {
          this.records[p][o] = new KafkaConsumerRecordImpl<>(
            new ConsumerRecord<>(
              "abc",
              p,
              o,
              null,
              null
            )
          );
        }
      }
    }

  }

  @Benchmark
  public void benchmarkReverseOrder(RecordsState recordsState, Blackhole blackhole) {
    OffsetManager offsetManager = new OffsetManager(Vertx.vertx(), new MockKafkaConsumer(), null, 10000L);

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
      for (int offset : new int[]{5, 2, 0, 7, 1, 3, 4, 6}) {
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

  static class MockKafkaConsumer implements io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> {
    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> exceptionHandler(
      Handler<Throwable> handler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> handler(
      Handler<KafkaConsumerRecord<String, CloudEvent>> handler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> pause() {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> resume() {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> fetch(long amount) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> endHandler(
      Handler<Void> endHandler) {
      return null;
    }

    @Override
    public long demand() {
      return 0;
    }

    @Override
    public Future<Void> subscribe(String topic) {
      return null;
    }

    @Override
    public Future<Void> subscribe(Set<String> topics) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> subscribe(
      String topic,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> subscribe(
      Set<String> topics,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public Future<Void> subscribe(Pattern pattern) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> subscribe(
      Pattern pattern,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public Future<Void> assign(TopicPartition topicPartition) {
      return null;
    }

    @Override
    public Future<Void> assign(
      Set<TopicPartition> topicPartitions) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> assign(
      TopicPartition topicPartition,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> assign(
      Set<TopicPartition> topicPartitions,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> assignment(
      Handler<AsyncResult<Set<TopicPartition>>> handler) {
      return null;
    }

    @Override
    public Future<Set<TopicPartition>> assignment() {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> listTopics(
      Handler<AsyncResult<Map<String, List<PartitionInfo>>>> handler) {
      return null;
    }

    @Override
    public Future<Map<String, List<PartitionInfo>>> listTopics() {
      return null;
    }

    @Override
    public Future<Void> unsubscribe() {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> unsubscribe(
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> subscription(
      Handler<AsyncResult<Set<String>>> handler) {
      return null;
    }

    @Override
    public Future<Set<String>> subscription() {
      return null;
    }

    @Override
    public Future<Void> pause(TopicPartition topicPartition) {
      return null;
    }

    @Override
    public Future<Void> pause(
      Set<TopicPartition> topicPartitions) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> pause(
      TopicPartition topicPartition,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> pause(
      Set<TopicPartition> topicPartitions,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public void paused(
      Handler<AsyncResult<Set<TopicPartition>>> handler) {

    }

    @Override
    public Future<Set<TopicPartition>> paused() {
      return null;
    }

    @Override
    public Future<Void> resume(TopicPartition topicPartition) {
      return null;
    }

    @Override
    public Future<Void> resume(
      Set<TopicPartition> topicPartitions) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> resume(
      TopicPartition topicPartition,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> resume(
      Set<TopicPartition> topicPartitions,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> partitionsRevokedHandler(
      Handler<Set<TopicPartition>> handler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> partitionsAssignedHandler(
      Handler<Set<TopicPartition>> handler) {
      return null;
    }

    @Override
    public Future<Void> seek(TopicPartition topicPartition, long offset) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> seek(
      TopicPartition topicPartition, long offset,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public Future<Void> seekToBeginning(TopicPartition topicPartition) {
      return null;
    }

    @Override
    public Future<Void> seekToBeginning(
      Set<TopicPartition> topicPartitions) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> seekToBeginning(
      TopicPartition topicPartition,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> seekToBeginning(
      Set<TopicPartition> topicPartitions,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public Future<Void> seekToEnd(TopicPartition topicPartition) {
      return null;
    }

    @Override
    public Future<Void> seekToEnd(
      Set<TopicPartition> topicPartitions) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> seekToEnd(
      TopicPartition topicPartition,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> seekToEnd(
      Set<TopicPartition> topicPartitions,
      Handler<AsyncResult<Void>> completionHandler) {
      return null;
    }

    @Override
    public Future<Void> commit() {
      return null;
    }

    @Override
    public void commit(
      Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public Future<Map<TopicPartition, OffsetAndMetadata>> commit(
      Map<TopicPartition, OffsetAndMetadata> offsets) {
      return Future.succeededFuture(offsets);
    }

    @Override
    public void commit(
      Map<TopicPartition, OffsetAndMetadata> offsets,
      Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
      completionHandler.handle(Future.succeededFuture(offsets));
    }

    @Override
    public void committed(TopicPartition topicPartition,
                          Handler<AsyncResult<OffsetAndMetadata>> handler) {

    }

    @Override
    public Future<OffsetAndMetadata> committed(
      TopicPartition topicPartition) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> partitionsFor(
      String topic,
      Handler<AsyncResult<List<PartitionInfo>>> handler) {
      return null;
    }

    @Override
    public Future<List<PartitionInfo>> partitionsFor(String topic) {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> batchHandler(
      Handler<KafkaConsumerRecords<String, CloudEvent>> handler) {
      return null;
    }

    @Override
    public Future<Void> close() {
      return null;
    }

    @Override
    public void close(
      Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public void position(TopicPartition partition,
                         Handler<AsyncResult<Long>> handler) {

    }

    @Override
    public Future<Long> position(TopicPartition partition) {
      return null;
    }

    @Override
    public void offsetsForTimes(
      Map<TopicPartition, Long> topicPartitionTimestamps,
      Handler<AsyncResult<Map<TopicPartition, OffsetAndTimestamp>>> handler) {

    }

    @Override
    public Future<Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(
      Map<TopicPartition, Long> topicPartitionTimestamps) {
      return null;
    }

    @Override
    public void offsetsForTimes(TopicPartition topicPartition, Long timestamp,
                                Handler<AsyncResult<OffsetAndTimestamp>> handler) {

    }

    @Override
    public Future<OffsetAndTimestamp> offsetsForTimes(
      TopicPartition topicPartition, Long timestamp) {
      return null;
    }

    @Override
    public void beginningOffsets(
      Set<TopicPartition> topicPartitions,
      Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {

    }

    @Override
    public Future<Map<TopicPartition, Long>> beginningOffsets(
      Set<TopicPartition> topicPartitions) {
      return null;
    }

    @Override
    public void beginningOffsets(TopicPartition topicPartition,
                                 Handler<AsyncResult<Long>> handler) {

    }

    @Override
    public Future<Long> beginningOffsets(TopicPartition topicPartition) {
      return null;
    }

    @Override
    public void endOffsets(Set<TopicPartition> topicPartitions,
                           Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {

    }

    @Override
    public Future<Map<TopicPartition, Long>> endOffsets(
      Set<TopicPartition> topicPartitions) {
      return null;
    }

    @Override
    public void endOffsets(TopicPartition topicPartition,
                           Handler<AsyncResult<Long>> handler) {

    }

    @Override
    public Future<Long> endOffsets(TopicPartition topicPartition) {
      return null;
    }

    @Override
    public KafkaReadStream<String, CloudEvent> asStream() {
      return null;
    }

    @Override
    public Consumer<String, CloudEvent> unwrap() {
      return null;
    }

    @Override
    public io.vertx.kafka.client.consumer.KafkaConsumer<String, CloudEvent> pollTimeout(
      Duration timeout) {
      return null;
    }

    @Override
    public void poll(Duration timeout,
                     Handler<AsyncResult<KafkaConsumerRecords<String, CloudEvent>>> handler) {

    }

    @Override
    public Future<KafkaConsumerRecords<String, CloudEvent>> poll(Duration timeout) {
      return null;
    }
  }

}
