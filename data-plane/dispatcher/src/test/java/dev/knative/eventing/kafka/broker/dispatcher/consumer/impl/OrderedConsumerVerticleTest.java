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
package dev.knative.eventing.kafka.broker.dispatcher.consumer.impl;

import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class OrderedConsumerVerticleTest extends AbstractConsumerVerticleTest {

  private static Stream<Arguments> inputArgs() {
    return Stream.of(
      Arguments.of(0L, 10),
      Arguments.of(0L, 100),
      Arguments.of(0L, 1000),
      Arguments.of(0L, 10000),
      Arguments.of(50L, 500),
      Arguments.of(100L, 10),
      Arguments.of(100L, 100),
      Arguments.of(1000L, 10)
    );
  }

  @Timeout(value = 20, timeUnit = TimeUnit.SECONDS) // Longest takes 10 secs
  @ParameterizedTest
  @MethodSource("inputArgs")
  public void consumeOneByOne(final long millis, final int tasks, final Vertx vertx) throws InterruptedException {
    final var topic = "topic1";
    final Random random = new Random();
    final var consumer = new MockConsumer<String, CloudEvent>(OffsetResetStrategy.LATEST);

    // Mock the record dispatcher to count down the latch and save the received records order
    CountDownLatch latch = new CountDownLatch(tasks);
    final Map<TopicPartition, List<Long>> receivedRecords = new HashMap<>();
    final var recordDispatcher = mock(RecordDispatcher.class);
    when(recordDispatcher.apply(any())).then(invocation -> {
      final KafkaConsumerRecord<String, CloudEvent> record = invocation.getArgument(0);
      return recordDispatcherLogicMock(vertx, random, millis, latch, record, receivedRecords);
    });
    when(recordDispatcher.close()).thenReturn(Future.succeededFuture());

    final var verticle = createConsumerVerticle(
      (vx, consumerVerticle) -> {
        consumerVerticle.setConsumer(KafkaConsumer.create(vx, consumer));
        consumerVerticle.setRecordDispatcher(recordDispatcher);
        consumerVerticle.setCloser(Future::succeededFuture);

        return Future.succeededFuture();
      }, Set.of(topic)
    );

    // Deploy the verticle
    CountDownLatch deployLatch = new CountDownLatch(1);
    vertx.deployVerticle(verticle).onComplete(v -> deployLatch.countDown());
    deployLatch.await();

    assertThat(consumer.subscription())
      .containsExactlyInAnyOrder(topic);
    assertThat(consumer.closed())
      .isFalse();

    // Assign partitions to consumer (required to add records)
    consumer.updateEndOffsets(
      IntStream.range(0, 10)
        .mapToObj(partition -> new org.apache.kafka.common.TopicPartition(topic, partition))
        .collect(Collectors.toMap(Function.identity(), v -> 0L))
    );
    consumer.rebalance(
      IntStream.range(0, 10)
        .mapToObj(partition -> new org.apache.kafka.common.TopicPartition(topic, partition))
        .collect(Collectors.toList())
    );

    // Add the records
    Map<Integer, Long> lastCommitted = new HashMap<>();
    for (int i = 0; i < tasks; i++) {
      int partition = (int) Math.round(Math.floor(random.nextDouble() * 10));
      long offset = lastCommitted.getOrDefault(partition, -1L);
      offset++;
      consumer.addRecord(record(topic, partition, offset));
      lastCommitted.put(partition, offset);
    }

    // Wait for all records to be processed
    latch.await();

    // Check they were received in order
    for (Map.Entry<TopicPartition, List<Long>> e : receivedRecords.entrySet()) {
      // Check if tasks were received in order
      List<Long> committed = e.getValue();
      LongStream.range(0, committed.size())
        .forEach(i -> {
          long actual = committed.get((int) i);
          assertThat(actual)
            .isEqualTo(i);
        });
    }

    // Check they are all received
    assertThat(receivedRecords
      .values()
      .stream()
      .mapToInt(List::size)
      .sum())
      .isEqualTo(tasks);
  }

  @Override
  BaseConsumerVerticle createConsumerVerticle(
    BaseConsumerVerticle.Initializer initializer, Set<String> topics) {
    return new OrderedConsumerVerticle(initializer, topics);
  }

  protected static ConsumerRecord<String, CloudEvent> record(String topic, int partition, long offset) {
    return new ConsumerRecord<>(
      topic,
      partition,
      offset,
      null,
      null
    );
  }

  private Future<Void> recordDispatcherLogicMock(
    Vertx vertx,
    Random random,
    long millis,
    CountDownLatch latch,
    KafkaConsumerRecord<String, CloudEvent> record,
    Map<TopicPartition, List<Long>> receivedRecords) {
    if (millis == 0) {
      if (latch != null) {
        latch.countDown();
      }
      receivedRecords
        .computeIfAbsent(new TopicPartition(record.topic(), record.partition()), tp -> new ArrayList<>())
        .add(record.offset());
      return Future.succeededFuture();
    } else {
      // Some random number around the provided millis
      long delay = Math.round(Math.ceil(millis + (random.nextDouble() * millis * 0.5)));

      Promise<Void> prom = Promise.promise();
      vertx.setTimer(delay, v -> {
        if (latch != null) {
          latch.countDown();
        }
        receivedRecords
          .computeIfAbsent(new TopicPartition(record.topic(), record.partition()), tp -> new ArrayList<>())
          .add(record.offset());
        prom.complete();
      });
      return prom.future();
    }
  }

}
