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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(value = ExecutionMode.CONCURRENT)
@ExtendWith(VertxExtension.class)
@DisabledIfEnvironmentVariable(named = "SKIP_SLOW_TESTS", matches = "true")
public class OffsetManagerTest extends AbstractOffsetManagerTest {

    @Override
    RecordDispatcherListener createOffsetManager(final Vertx vertx, final ReactiveKafkaConsumer<?, ?> consumer) {
        return new OffsetManager(vertx, consumer, null, 100L);
    }

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
    public void shouldCommitAfterSendingEventsOrderedOnTheSamePartitionLongValues() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    for (long i = Integer.MAX_VALUE - 50; i < ((long) Integer.MAX_VALUE) + 50; i++) {
                        var rec = record("aaa", 0, i);
                        offsetStrategy.recordReceived(rec);
                        offsetStrategy.successfullySentToSubscriber(rec);
                    }
                })
                .containsEntry(new TopicPartition("aaa", 0), Integer.MAX_VALUE + 50L);
    }

    @Test
    public void shouldCommitAfterSendingEventsOrderedOnTheSamePartitionLongPeriod() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    for (long i = 0; i < 2_000_051; i++) {

                        var rec = record("aaa", 0, i);
                        offsetStrategy.recordReceived(rec);
                        offsetStrategy.successfullySentToSubscriber(rec);
                    }
                })
                .containsEntry(new TopicPartition("aaa", 0), 2_000_051L);
    }

    @Test
    public void shouldNotCommitAfterSendingEventsOrderedOnTheSamePartitionBrokenSequence() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    // start number is odd number
                    for (long i = Integer.MAX_VALUE - 50; i < ((long) Integer.MAX_VALUE) + 50; i++) {
                        var rec = record("aaa", 0, i);
                        offsetStrategy.recordReceived(rec);
                        if (i % 2 == 0) {
                            offsetStrategy.successfullySentToSubscriber(rec);
                        }
                    }
                })
                .isEmpty();
    }

    @Test
    public void shouldNotCommitAndNotGoOutOfBounds() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    offsetStrategy.recordReceived(record("aaa", 0, 0));
                    offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 64));
                    offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 128));
                })
                .isEmpty();
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
        assertThatOffsetCommitted(
                        List.of(new TopicPartition("aaa", 0), new TopicPartition("aaa", 1)), offsetStrategy -> {
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
                .containsEntry(new TopicPartition("aaa", 0), 6L);
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
    public void shouldCommitSuccessfullyOnSuccessfullySentToDeadLetterSink() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    var rec = record("aaa", 0, 0);
                    offsetStrategy.recordReceived(rec);
                    offsetStrategy.successfullySentToDeadLetterSink(rec);
                })
                .containsEntry(new TopicPartition("aaa", 0), 1L);
    }

    @Test
    public void shouldCommitSuccessfullyWithRecordDiscardedInTheMiddle() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    var rec = record("aaa", 0, 0);
                    offsetStrategy.recordReceived(rec);
                    offsetStrategy.recordDiscarded(rec);
                    offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 1));
                })
                .containsEntry(new TopicPartition("aaa", 0), 2L);
    }

    @Test
    public void shouldCommitSuccessfullyWithRecordFailedToDeadLetterSinkInTheMiddle() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    var rec = record("aaa", 0, 0);
                    offsetStrategy.recordReceived(rec);
                    offsetStrategy.failedToSendToDeadLetterSink(rec, new IllegalStateException());
                    offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 1));
                })
                .containsEntry(new TopicPartition("aaa", 0), 2L);
    }

    @Test
    public void shouldContinueToWorkAfterSendingALotOfRecords() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    offsetStrategy.recordReceived(record("aaa", 0, 0));
                    for (int i = 128 * 64 - 1; i >= 0; i--) {
                        offsetStrategy.successfullySentToSubscriber(record("aaa", 0, i));
                    }
                    offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 128L * 64L));
                })
                .containsEntry(new TopicPartition("aaa", 0), 128L * 64L + 1L);
    }

    @Test
    public void shouldContinueToWorkAfterResetWithOutOfOrderEventsOnANon64BitBoundary() {
        assertThatOffsetCommitted(List.of(new TopicPartition("aaa", 0)), offsetStrategy -> {
                    offsetStrategy.recordReceived(record("aaa", 0, 0));

                    // This loop will flag 1 bit in the BitSet of the OffsetTracker beyond a 64-bit
                    // word boundary.
                    for (int i = 0; i < (1_000_000 - 64 + 1); i++) {
                        offsetStrategy.successfullySentToSubscriber(record("aaa", 0, i));
                    }

                    // The rest of the BitSet until the reset threshold of 1_000_000 is set with
                    // alternating 1/0.
                    for (int i = (1_000_000 - 64 + 2); i <= 1_000_000; i += 2) {
                        offsetStrategy.successfullySentToSubscriber(record("aaa", 0, i));
                    }

                    // Now we need to make sure that our offset got committed.
                    try {
                        Thread.sleep(1000);
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    // Now set one of the bits beyond the 1_000_000 mark, resetting the
                    // OffsetTracker.
                    offsetStrategy.successfullySentToSubscriber(record("aaa", 0, 1_000_001));

                    // Now fill up the gaps in the BitSet of the OffsetTracker.
                    for (int i = (1_000_000 - 64 + 1); i <= 1_000_000; i += 2) {
                        offsetStrategy.successfullySentToSubscriber(record("aaa", 0, i));
                    }
                })
                .containsEntry(new TopicPartition("aaa", 0), 1_000_002L);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void recordReceived(final Vertx vertx) {
        final ReactiveKafkaConsumer<String, CloudEvent> consumer = mock(ReactiveKafkaConsumer.class);
        final Counter eventsSentCounter = mock(Counter.class);
        new OffsetManager(vertx, consumer, eventsSentCounter::increment, 100L).recordReceived(record("aaa", 0, 0));

        shouldNeverCommit(consumer);
        shouldNeverPause(consumer);
        verify(eventsSentCounter, never()).increment();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void failedToSendToDeadLetterSink(final Vertx vertx) {
        final ReactiveKafkaConsumer<String, CloudEvent> consumer = mock(ReactiveKafkaConsumer.class);
        final Counter eventsSentCounter = mock(Counter.class);

        OffsetManager strategy = new OffsetManager(vertx, consumer, eventsSentCounter::increment, 100L);
        strategy.recordReceived(record("aaa", 0, 0));
        strategy.failedToSendToDeadLetterSink(record("aaa", 0, 0), null);

        shouldNeverCommit(consumer);
        shouldNeverPause(consumer);
        verify(eventsSentCounter, never()).increment();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFailedCommitRetry(final Vertx vertx) {
        final var counter = new AtomicInteger(0);
        final Counter eventsSentCounter = mock(Counter.class);

        final ReactiveKafkaConsumer<String, CloudEvent> consumer = mock(ReactiveKafkaConsumer.class);
        when(consumer.commit((Map<TopicPartition, OffsetAndMetadata>) any())).then(invocationOnMock -> {
            if (counter.incrementAndGet() == 1) {
                return Future.failedFuture(new RuntimeException());
            }
            return Future.succeededFuture();
        });

        final var r = record("aaa", 0, 0);

        OffsetManager strategy = new OffsetManager(vertx, consumer, eventsSentCounter::increment, 100L);
        strategy.recordReceived(r);
        strategy.successfullySentToSubscriber(r);

        final var offset = strategy.getOffsetTrackers().get(new TopicPartition(r.topic(), r.partition()));

        await().timeout(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(offset.getCommitted()).isEqualTo(1));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAlwaysFailedCommit(final Vertx vertx) {
        final var counter = new AtomicInteger(0);
        final Counter eventsSentCounter = mock(Counter.class);

        final ReactiveKafkaConsumer<String, CloudEvent> consumer = mock(ReactiveKafkaConsumer.class);
        when(consumer.commit((Map<TopicPartition, OffsetAndMetadata>) any()))
                .then(invocationOnMock -> Future.failedFuture(new RuntimeException()));

        final var r = record("aaa", 0, 0);

        OffsetManager strategy = new OffsetManager(vertx, consumer, eventsSentCounter::increment, 100L);
        strategy.recordReceived(r);
        strategy.successfullySentToSubscriber(r);

        final var offset = strategy.getOffsetTrackers().get(new TopicPartition(r.topic(), r.partition()));

        await().timeout(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(offset.getCommitted()).isEqualTo(0));
    }
}
