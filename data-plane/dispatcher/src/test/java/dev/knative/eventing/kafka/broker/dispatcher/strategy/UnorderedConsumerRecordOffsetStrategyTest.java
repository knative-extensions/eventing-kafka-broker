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
package dev.knative.eventing.kafka.broker.dispatcher.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.MapAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(value = ExecutionMode.CONCURRENT)
public class UnorderedConsumerRecordOffsetStrategyTest {

  @Test
  public void shouldCommitAfterSendingEventsOrderedOnTheSamePartition() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));
      for (int i = 0; i < 10; i++) {
        var rec = record("aaa", 0, i);
        offsetStrategy.successfullySentToSubscriber(rec);
      }
    })
      .containsEntry(new TopicPartition("aaa", 0), 10L);
  }

  @Test
  public void shouldCommitAfterSendingEventsOrderedOnTheSamePartitionWithInducedFailure() {
    assertThatOffsetCommittedWithFailures(List.of(new TopicPartition("aaa", 0)), (offsetStrategy, failureFlag) -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));
      failureFlag.set(true);
      for (int i = 0; i < 10; i++) {
        var rec = record("aaa", 0, i);
        offsetStrategy.successfullySentToSubscriber(rec);
      }
      failureFlag.set(false);
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 10));
    })
      .containsEntry(new TopicPartition("aaa", 0), 11L);
  }

  @Test
  public void shouldCommitInAMixedOrderWithInducedFailure() {
    assertThatOffsetCommittedWithFailures(List.of(new TopicPartition("aaa", 0)), (offsetStrategy, failureFlag) -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));

      // Order:
      // 0 2 1
      // flip failure flag
      // 4 3
      // flip failure flag
      // 6 5
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 0));
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 2));
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 1));
      failureFlag.set(true);
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 4));
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 3));
      failureFlag.set(false);
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 6));
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 5));
    })
      .containsEntry(new TopicPartition("aaa", 0), 7L);
  }

  @Test
  public void shouldCommitAfterSendingEventsOrderedOnDifferentPartitions() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0), new TopicPartition("aaa", 1)), offsetStrategy -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));
      offsetStrategy.recordReceived(record("aaa", 1, 0));
      for (int i = 0; i < 10; i++) {
        var rec = record("aaa", i % 2, (long) Math.floor((double) i / 2));
        offsetStrategy.successfullySentToSubscriber(rec);
      }
    })
      .containsEntry(new TopicPartition("aaa", 0), 5L)
      .containsEntry(new TopicPartition("aaa", 1), 5L);
  }

  @Test
  public void shouldCommitAfterSendingEventsABitMixed() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));
      for (int i = 0; i < 12; i++) {
        // This will commit in the following order:
        // 1 0 3 2 5 4
        var rec = record("aaa", 0, i % 2 == 0 ? i + 1 : i - 1);
        offsetStrategy.successfullySentToSubscriber(rec);
      }
    })
      .containsEntry(new TopicPartition("aaa", 0), 12L);
  }

  @Test
  public void shouldCommitAfterSendingEventsABitMoreMixed() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));
      // This will commit in the following order:
      // 5 2 0 7 1 3 4 6
      List.of(5L, 2L, 0L, 7L, 1L, 3L, 4L, 6L)
        .forEach(offset -> offsetStrategy.successfullySentToSubscriber(record("aaa", 0, offset)));
    })
      .containsEntry(new TopicPartition("aaa", 0), 8L);
  }

  @Test
  public void shouldNotCommitAfterSendingEventsABitMoreMixedWithAMissingOne() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));
      // This will commit in the following order:
      // 5 2 0 7 1 3 4
      List.of(5L, 2L, 0L, 7L, 1L, 3L, 4L)
        .forEach(offset -> offsetStrategy.successfullySentToSubscriber(record("aaa", 0, offset)));
    })
      .isEmpty();
  }

  @Test
  public void shouldCommitOnlyPartiallyAfterSendingEventsABitMoreMixed() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));
      // This will commit in the following order:
      // 5 2 0 1 3 4 7 8
      List.of(5L, 2L, 0L, 1L, 3L, 4L, 7L, 8L)
        .forEach(offset -> offsetStrategy.successfullySentToSubscriber(record("aaa", 0, offset)));
    })
      .containsEntry(new TopicPartition("aaa", 0), 6L);
  }

  @Test
  public void shouldCommitSuccessfullyOnSuccessfullySentToDLQ() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      var rec = record("aaa", 0, 0);
      offsetStrategy.recordReceived(rec);
      offsetStrategy.successfullySentToDLQ(rec);
    })
      .containsEntry(new TopicPartition("aaa", 0), 1L);
  }

  @Test
  public void ShouldCommitSuccessfullyOnRecordDiscarded() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      var rec = record("aaa", 0, 0);
      offsetStrategy.recordReceived(rec);
      offsetStrategy.recordDiscarded(rec);
    })
      .containsEntry(new TopicPartition("aaa", 0), 1L);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void recordReceived() {
    final KafkaConsumer<String, CloudEvent> consumer = mock(KafkaConsumer.class);
    final Counter eventsSentCounter = mock(Counter.class);
    new UnorderedConsumerRecordOffsetStrategy(consumer, eventsSentCounter).recordReceived(record("aaa", 0, 0));

    shouldNeverCommit(consumer);
    shouldNeverPause(consumer);
    verify(eventsSentCounter, never()).increment();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void failedToSendToDLQ() {
    final KafkaConsumer<String, CloudEvent> consumer = mock(KafkaConsumer.class);
    final Counter eventsSentCounter = mock(Counter.class);

    new UnorderedConsumerRecordOffsetStrategy(consumer, eventsSentCounter).failedToSendToDLQ(null, null);

    shouldNeverCommit(consumer);
    shouldNeverPause(consumer);
    verify(eventsSentCounter, never()).increment();
  }

  private static MapAssert<TopicPartition, Long> assertThatOffsetCommitted(
    Collection<TopicPartition> partitionsConsumed, Consumer<UnorderedConsumerRecordOffsetStrategy> testExecutor) {
    return assertThatOffsetCommittedWithFailures(partitionsConsumed,
      (offsetStrategy, flag) -> testExecutor.accept(offsetStrategy));
  }

  private static MapAssert<TopicPartition, Long> assertThatOffsetCommittedWithFailures(
    Collection<TopicPartition> partitionsConsumed,
    BiConsumer<UnorderedConsumerRecordOffsetStrategy, AtomicBoolean> testExecutor) {
    final var mockConsumer = new MockConsumer<String, CloudEvent>(OffsetResetStrategy.NONE);
    mockConsumer.assign(partitionsConsumed);

    // Funky flag to flip in order to induce a failure
    AtomicBoolean failureFlag = new AtomicBoolean(false);

    final KafkaConsumer<String, CloudEvent> vertxConsumer = mock(KafkaConsumer.class);
    doAnswer(invocation -> {
      if (failureFlag.get()) {
        return Future.failedFuture("some failure");
      }
      // If you don't want to lose hours in debugging, please don't remove this FQCNs :)
      final Map<io.vertx.kafka.client.common.TopicPartition, io.vertx.kafka.client.consumer.OffsetAndMetadata>
        topicsPartitions = invocation.getArgument(0);
      mockConsumer.commitSync(
        topicsPartitions.entrySet()
          .stream()
          .collect(Collectors.toMap(
            e -> new TopicPartition(e.getKey().getTopic(), e.getKey().getPartition()),
            e -> new OffsetAndMetadata(e.getValue().getOffset(), e.getValue().getMetadata())
          ))
      );
      return Future.succeededFuture();
    })
      .when(vertxConsumer)
      .commit(any(Map.class));

    testExecutor.accept(new UnorderedConsumerRecordOffsetStrategy(vertxConsumer, new CumulativeCounter(mock(Id.class))),
      failureFlag);

    return assertThat(
      mockConsumer.committed(Set.copyOf(partitionsConsumed))
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().offset()))
    );
  }

  private static KafkaConsumerRecord<String, CloudEvent> record(String topic, int partition, long offset) {
    return new KafkaConsumerRecordImpl<>(
      new ConsumerRecord<>(
        topic,
        partition,
        offset,
        null,
        null
      )
    );
  }

  @SuppressWarnings("unchecked")
  private static void shouldNeverCommit(final KafkaConsumer<String, CloudEvent> consumer) {
    verify(consumer, never()).commit();
    verify(consumer, never()).commit(any(Handler.class));
    verify(consumer, never()).commit(any(Map.class));
    verify(consumer, never()).commit(any(), any());
  }

  @SuppressWarnings("unchecked")
  private static void shouldNeverPause(final KafkaConsumer<String, CloudEvent> consumer) {
    verify(consumer, never()).pause();
    verify(consumer, never()).pause(any(io.vertx.kafka.client.common.TopicPartition.class));
    verify(consumer, never()).pause(any(Set.class));
    verify(consumer, never()).pause(any(io.vertx.kafka.client.common.TopicPartition.class), any());
    verify(consumer, never()).pause(any(Set.class), any());
  }
}
