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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import dev.knative.eventing.kafka.broker.core.ReactiveKafkaConsumer;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.AbstractMap;
import java.util.Collection;
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

public abstract class AbstractOffsetManagerTest {

    abstract RecordDispatcherListener createOffsetManager(
            final Vertx vertx,
            final ReactiveKafkaConsumer<?, ?> consumer,
            Collection<TopicPartition> partitionsConsumed);

    protected static ConsumerRecord<String, CloudEvent> record(String topic, int partition, long offset) {
        return new ConsumerRecord<>(topic, partition, offset, null, null);
    }

    protected MapAssert<TopicPartition, Long> assertThatOffsetCommitted(
            final Collection<TopicPartition> partitionsConsumed,
            final long initialOffset,
            final Consumer<RecordDispatcherListener> testExecutor) {
        return assertThatOffsetCommittedWithFailures(
                partitionsConsumed, initialOffset, (offsetStrategy, flag) -> testExecutor.accept(offsetStrategy));
    }

    protected MapAssert<TopicPartition, Long> assertThatOffsetCommittedWithFailures(
            Collection<TopicPartition> partitionsConsumed,
            final long initialOffset,
            BiConsumer<RecordDispatcherListener, AtomicBoolean> testExecutor) {
        final var mockConsumer = new MockConsumer<String, CloudEvent>(OffsetResetStrategy.NONE);
        mockConsumer.assign(partitionsConsumed);
        if (initialOffset > 0) {
            mockConsumer.commitSync(partitionsConsumed.stream()
                    .map(tp -> new AbstractMap.SimpleEntry<>(tp, new OffsetAndMetadata(initialOffset - 1)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }

        // Funky flag to flip in order to induce a failure
        AtomicBoolean failureFlag = new AtomicBoolean(false);

        final ReactiveKafkaConsumer<String, CloudEvent> vertxConsumer = mock(ReactiveKafkaConsumer.class);
        doAnswer(invocation -> {
                    if (failureFlag.get()) {
                        return Future.failedFuture("some failure");
                    }
                    // If you don't want to lose hours in debugging, please don't remove this FQCNs :)
                    final Map<TopicPartition, OffsetAndMetadata> topicsPartitions = invocation.getArgument(0);
                    mockConsumer.commitSync(topicsPartitions);
                    return Future.succeededFuture();
                })
                .when(vertxConsumer)
                .commit(any(Map.class));

        final var offsetManager = createOffsetManager(Vertx.vertx(), vertxConsumer, partitionsConsumed);
        testExecutor.accept(offsetManager, failureFlag);

        try {
            Thread.sleep(1000);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertThat(offsetManager.close().succeeded()).isTrue();

        final var committed = mockConsumer.committed(Set.copyOf(partitionsConsumed));
        return assertThat(committed.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().offset())));
    }

    @SuppressWarnings("unchecked")
    protected void shouldNeverCommit(final ReactiveKafkaConsumer<String, CloudEvent> consumer) {
        verify(consumer, never()).commit(any(Map.class));
    }

    @SuppressWarnings("unchecked")
    protected void shouldNeverPause(final ReactiveKafkaConsumer<String, CloudEvent> consumer) {
        verify(consumer, never()).pause(any(Collection.class));
    }
}
