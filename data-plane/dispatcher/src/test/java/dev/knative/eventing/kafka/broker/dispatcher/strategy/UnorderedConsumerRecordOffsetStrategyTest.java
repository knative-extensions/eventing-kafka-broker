/*
 * Copyright 2020 The Knative Authors
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


import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

@ExtendWith(VertxExtension.class)
@Execution(value = ExecutionMode.CONCURRENT)
public class UnorderedConsumerRecordOffsetStrategyTest {

  private static final int TIMEOUT_MS = 200;

  @Test
  @SuppressWarnings("unchecked")
  public void recordReceived() {
    final KafkaConsumer<Object, Object> consumer = mock(KafkaConsumer.class);
    new UnorderedConsumerRecordOffsetStrategy<>(consumer).recordReceived(null);

    shouldNeverCommit(consumer);
    shouldNeverPause(consumer);
  }

  @Test
  public void shouldCommitSuccessfullyOnSuccessfullySentToSubscriber(final Vertx vertx) {
    shouldCommit(vertx, (kafkaConsumerRecord, unorderedConsumerOffsetManager)
        -> unorderedConsumerOffsetManager.successfullySentToSubscriber(kafkaConsumerRecord));
  }

  @Test
  public void shouldCommitSuccessfullyOnSuccessfullySentToDLQ(final Vertx vertx) {
    shouldCommit(vertx, (kafkaConsumerRecord, unorderedConsumerOffsetManager)
        -> unorderedConsumerOffsetManager.successfullySentToDLQ(kafkaConsumerRecord));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void failedToSendToDLQ() {
    final KafkaConsumer<Object, Object> consumer = mock(KafkaConsumer.class);
    new UnorderedConsumerRecordOffsetStrategy<>(consumer).failedToSendToDLQ(null, null);

    shouldNeverCommit(consumer);
    shouldNeverPause(consumer);
  }

  @Test
  public void ShouldCommitSuccessfullyOnRecordDiscarded(final Vertx vertx) {
    shouldCommit(vertx, (kafkaConsumerRecord, unorderedConsumerOffsetManager)
        -> unorderedConsumerOffsetManager.recordDiscarded(kafkaConsumerRecord));
  }

  @SuppressWarnings("unchecked")
  private static <K, V> void shouldCommit(
      final Vertx vertx,
      final BiConsumer<KafkaConsumerRecord<K, V>, UnorderedConsumerRecordOffsetStrategy<K, V>> rConsumer) {

    final var topic = "topic-42";
    final var partition = 42;
    final var offset = 142;
    final var partitions = new HashSet<TopicPartition>();
    final var topicPartition = new TopicPartition(topic, partition);
    partitions.add(topicPartition);

    final var mockConsumer = new MockConsumer<K, V>(OffsetResetStrategy.LATEST);
    mockConsumer.assign(partitions);

    final KafkaConsumer<K, V> consumer = (KafkaConsumer<K, V>) Mockito.mock(KafkaConsumer.class);
    doAnswer(invocation -> {

      final Map<io.vertx.kafka.client.common.TopicPartition, io.vertx.kafka.client.consumer.OffsetAndMetadata>
          topicsPartitions = invocation
          .getArgument(0);

      final var tp = topicsPartitions.entrySet().iterator().next();

      mockConsumer.commitSync(Map.of(
          new TopicPartition(tp.getKey().getTopic(), tp.getKey().getPartition()),
          new OffsetAndMetadata(tp.getValue().getOffset(), tp.getValue().getMetadata())
      ));

      return Future.succeededFuture();
    }).when(consumer).commit(
        (Map<io.vertx.kafka.client.common.TopicPartition, io.vertx.kafka.client.consumer.OffsetAndMetadata>) any());

    final var offsetManager = new UnorderedConsumerRecordOffsetStrategy<>(consumer);
    final var record = new KafkaConsumerRecordImpl<>(
        new ConsumerRecord<K, V>(
            topic,
            partition,
            offset,
            null,
            null
        )
    );

    rConsumer.accept(record, offsetManager);

    final var committed = mockConsumer.committed(partitions);

    assertThat(committed).hasSize(1);
    assertThat(committed.keySet()).containsExactlyInAnyOrder(topicPartition);
    assertThat(committed.values()).containsExactlyInAnyOrder(new OffsetAndMetadata(offset + 1, ""));
  }

  @SuppressWarnings("unchecked")
  private static <K, V> void shouldNeverCommit(final KafkaConsumer<K, V> consumer) {
    verify(consumer, never()).commit();
    verify(consumer, never()).commit(any(Handler.class));
    verify(consumer, never()).commit(any(Map.class));
    verify(consumer, never()).commit(any(), any());
  }

  @SuppressWarnings("unchecked")
  private static <K, V> void shouldNeverPause(final KafkaConsumer<K, V> consumer) {
    verify(consumer, never()).pause();
    verify(consumer, never()).pause(any(io.vertx.kafka.client.common.TopicPartition.class));
    verify(consumer, never()).pause(any(Set.class));
    verify(consumer, never()).pause(any(io.vertx.kafka.client.common.TopicPartition.class), any());
    verify(consumer, never()).pause(any(Set.class), any());
  }
}
