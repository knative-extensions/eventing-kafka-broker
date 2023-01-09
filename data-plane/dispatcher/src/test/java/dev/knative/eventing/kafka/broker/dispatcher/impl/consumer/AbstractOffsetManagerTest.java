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

import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.MapAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public abstract class AbstractOffsetManagerTest {

  abstract RecordDispatcherListener createOffsetManager(final Vertx vertx, final KafkaConsumer<?, ?> consumer);

  protected static KafkaConsumerRecord<String, CloudEvent> record(String topic, int partition, long offset) {
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

  protected MapAssert<TopicPartition, Long> assertThatOffsetCommitted(
    final Collection<TopicPartition> partitionsConsumed,
    final Consumer<RecordDispatcherListener> testExecutor) {
    return assertThatOffsetCommittedWithFailures(partitionsConsumed,
      (offsetStrategy, flag) -> testExecutor.accept(offsetStrategy));
  }

  protected MapAssert<TopicPartition, Long> assertThatOffsetCommittedWithFailures(
    Collection<TopicPartition> partitionsConsumed,
    BiConsumer<RecordDispatcherListener, AtomicBoolean> testExecutor) {
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
      final Map<io.vertx.kafka.client.common.TopicPartition, OffsetAndMetadata>
        topicsPartitions = invocation.getArgument(0);
      mockConsumer.commitSync(
        topicsPartitions.entrySet()
          .stream()
          .collect(Collectors.toMap(
            e -> new TopicPartition(e.getKey().getTopic(), e.getKey().getPartition()),
            e -> new org.apache.kafka.clients.consumer.OffsetAndMetadata(e.getValue().getOffset(),
              e.getValue().getMetadata())
          ))
      );
      return Future.succeededFuture();
    })
      .when(vertxConsumer)
      .commit(any(Map.class));

    final var offsetManager = createOffsetManager(Vertx.vertx(), vertxConsumer);
    testExecutor.accept(offsetManager, failureFlag);

    try {
      Thread.sleep(1000);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertThat(offsetManager.close().succeeded()).isTrue();

    final var committed = mockConsumer.committed(Set.copyOf(partitionsConsumed));
    return assertThat(
      committed
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().offset()))
    );
  }

  @SuppressWarnings("unchecked")
  protected void shouldNeverCommit(final KafkaConsumer<String, CloudEvent> consumer) {
    verify(consumer, never()).commit();
    verify(consumer, never()).commit(any(Handler.class));
    verify(consumer, never()).commit(any(Map.class));
    verify(consumer, never()).commit(any(), any());
  }

  @SuppressWarnings("unchecked")
  protected void shouldNeverPause(final KafkaConsumer<String, CloudEvent> consumer) {
    verify(consumer, never()).pause();
    verify(consumer, never()).pause(any(io.vertx.kafka.client.common.TopicPartition.class));
    verify(consumer, never()).pause(any(Set.class));
    verify(consumer, never()).pause(any(io.vertx.kafka.client.common.TopicPartition.class), any());
    verify(consumer, never()).pause(any(Set.class), any());
  }
}
