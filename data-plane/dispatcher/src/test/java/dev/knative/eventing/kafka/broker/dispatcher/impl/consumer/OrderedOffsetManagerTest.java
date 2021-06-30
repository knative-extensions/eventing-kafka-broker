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
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(value = ExecutionMode.CONCURRENT)
public class OrderedOffsetManagerTest extends AbstractOffsetManagerTest {

  @Override
  RecordDispatcherListener createOffsetManager(
    KafkaConsumer<?, ?> consumer) {
    return new OrderedOffsetManager(consumer, null);
  }

  @Test
  public void shouldNotCommitAfterRecordReceived() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.recordReceived(record("aaa", 0, 0));
    }).isEmpty();
  }

  @Test
  public void shouldNotCommitAfterFailedToSendToDeadLetterSink() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.failedToSendToDeadLetterSink(record("aaa", 0, 0), new IllegalStateException());
    }).isEmpty();
  }

  @Test
  public void shouldNotCommitAfterRecordDiscarded() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.recordDiscarded(record("aaa", 0, 0));
    }).isEmpty();
  }


  @Test
  public void shouldCommitAfterSuccessfullySentToSubscriber() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 0));
    }).containsEntry(new TopicPartition("aaa", 0), 1L);
  }

  @Test
  public void shouldCommitAfterSuccessfullySentToDeadLetterSink() {
    assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
      offsetStrategy.successfullySentToDeadLetterSink(record("aaa", 0, 0));
    }).containsEntry(new TopicPartition("aaa", 0), 1L);
  }

}
