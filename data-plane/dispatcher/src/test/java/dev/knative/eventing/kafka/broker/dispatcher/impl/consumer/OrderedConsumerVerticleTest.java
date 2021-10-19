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

import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherImpl;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
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
      Arguments.of(0L, 10, 10, true),
      Arguments.of(0L, 100, 10, true),
      Arguments.of(0L, 1_000, 10, true),
      Arguments.of(0L, 10_000, 10, true),
      Arguments.of(50L, 500, 10, true),
      Arguments.of(50L, 100_000, 1_000, true),
      Arguments.of(50L, 100_000, 1_000, false),
      Arguments.of(100L, 10, 10, true),
      Arguments.of(100L, 100, 10, true),
      Arguments.of(1000L, 10, 10, true),
      Arguments.of(1000L, 100, 10, false), // ~10 seconds +- 5 seconds
      Arguments.of(500L, 10_000 * 4, 10_000, false)
    );
  }

  @ParameterizedTest(name = "with delay {0}ms, {1} tasks, {2} partitions and random partition assignment {3}")
  @MethodSource("inputArgs")
  public void consumeOneByOne(final long delay, final int tasks, final int partitions, final boolean randomAssignment,
                              final Vertx vertx) throws InterruptedException {
    final var topic = "topic1";
    final Random random = new Random();
    final var consumer = new MockConsumer<Object, CloudEvent>(OffsetResetStrategy.LATEST);

    // Mock the record dispatcher to count down the latch and save the received records order
    CountDownLatch latch = new CountDownLatch(tasks);
    final Map<TopicPartition, List<Long>> receivedRecords = new HashMap<>(partitions);
    final var recordDispatcher = mock(RecordDispatcherImpl.class);
    when(recordDispatcher.dispatch(any())).then(invocation -> {
      final KafkaConsumerRecord<String, CloudEvent> record = invocation.getArgument(0);
      return recordDispatcherLogicMock(vertx, random, delay, latch, record, receivedRecords);
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
      IntStream.range(0, partitions)
        .mapToObj(partition -> new org.apache.kafka.common.TopicPartition(topic, partition))
        .collect(Collectors.toMap(Function.identity(), v -> 0L))
    );
    consumer.rebalance(
      IntStream.range(0, partitions)
        .mapToObj(partition -> new org.apache.kafka.common.TopicPartition(topic, partition))
        .collect(Collectors.toList())
    );

    // Add the records
    Map<Integer, Long> lastCommitted = new HashMap<>(partitions);
    for (int i = 0; i < tasks; i++) {
      int partition = randomAssignment ?
        (int) Math.round(Math.floor(random.nextDouble() * partitions)) :
        i % partitions;
      long offset = lastCommitted.getOrDefault(partition, -1L);
      offset++;
      consumer.addRecord(record(topic, partition, offset));
      lastCommitted.put(partition, offset);
    }

    // Wait for all records to be processed
    assertThat(
      latch.await(60, TimeUnit.SECONDS)
    ).isTrue();

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
    assertThat(
      receivedRecords
        .values()
        .stream()
        .mapToInt(List::size)
        .sum()
    ).isEqualTo(tasks);
  }

  @Override
  BaseConsumerVerticle createConsumerVerticle(
    BaseConsumerVerticle.Initializer initializer, Set<String> topics) {
    return new OrderedConsumerVerticle(initializer, topics);
  }

  protected static ConsumerRecord<Object, CloudEvent> record(String topic, int partition, long offset) {
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
      receivedRecords
        .computeIfAbsent(new TopicPartition(record.topic(), record.partition()), tp -> new ArrayList<>())
        .add(record.offset());
      latch.countDown();
      return Future.succeededFuture();
    } else {
      // Some random number around the provided millis
      long delay = Math.round(Math.ceil(millis + (random.nextDouble() * millis * 0.5)));

      Promise<Void> prom = Promise.promise();
      vertx.setTimer(delay, v -> {
        receivedRecords
          .computeIfAbsent(new TopicPartition(record.topic(), record.partition()), tp -> new ArrayList<>())
          .add(record.offset());
        latch.countDown();
        prom.complete();
      });
      return prom.future();
    }
  }

}
