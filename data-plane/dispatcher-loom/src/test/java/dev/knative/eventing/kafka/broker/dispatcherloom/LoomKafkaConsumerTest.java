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
package dev.knative.eventing.kafka.broker.dispatcherloom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class LoomKafkaConsumerTest {

    private Vertx vertx;
    private LoomKafkaConsumer<String, Integer> consumer;
    private MockConsumer<String, Integer> mockConsumer;

    @BeforeEach
    public void setUp() {
        vertx = Vertx.vertx();
        mockConsumer = new MockConsumer<String, Integer>(OffsetResetStrategy.LATEST);
        consumer = new LoomKafkaConsumer<String, Integer>(vertx, mockConsumer);
    }

    @AfterEach
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testTaskRunnerThreadIsStopAfterClose(VertxTestContext testContext) throws InterruptedException {

        final var checkpoints = testContext.checkpoint();

        // add task to TaskRunnerThread
        consumer.subscribe(Collections.singletonList("test-topic"));
        consumer.poll(Duration.ofMillis(1000));
        consumer.commit(new HashMap<>());

        // Verify that the task runner thread is running
        assertTrue(consumer.isTaskRunnerThreadAlive());

        // Close the consumer
        consumer.close()
                .onComplete(ar -> {
                    ar.succeeded();
                    // Verify that the task runner thread is stopped
                    assertFalse(consumer.isTaskRunnerThreadAlive());
                    checkpoints.flag();
                })
                .onFailure(testContext::failNow);
    }

    @Test
    public void checkTaskQueueIsEmptyBeforeClose(VertxTestContext testContext) {

        final var checkpoints = testContext.checkpoint();

        // add task to TaskQueue
        consumer.subscribe(Collections.singletonList("test-topic"));
        consumer.poll(Duration.ofMillis(1000));
        consumer.commit(new HashMap<>());

        // Close the consumer
        consumer.close()
                .onComplete(ar -> {
                    ar.succeeded();
                    // Verify that the task queue is empty
                    assertEquals(0, consumer.getTaskQueueSize());
                    checkpoints.flag();
                })
                .onFailure(testContext::failNow);
    }

    @Test
    public void testQueueAfterClose(VertxTestContext testContext) {

        // close the consumer
        consumer.close().onFailure(testContext::failNow);

        // try to call method after close
        consumer.subscribe(Collections.singletonList("test-topic")).onComplete(ar -> {
            testContext.verify(() -> {
                assertTrue(ar.failed());
                assertEquals("Consumer is closed", ar.cause().getMessage());
                testContext.completeNow();
            });
        });
    }

    @Test
    public void testSubscribeAndPoll(VertxTestContext testContext) {
        // Test data
        final String topic = "test-topic";
        final Duration duration = Duration.ofMillis(1000);

        final var checkpoints = testContext.checkpoint(2);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topic))
                .onComplete(ar -> {
                    ar.succeeded();
                    // verify that the consumer is subscribed to the topic
                    assertEquals(1, mockConsumer.subscription().size());
                    assertTrue(mockConsumer.subscription().contains(topic));
                    checkpoints.flag();
                })
                .onFailure(testContext::failNow);

        final var rec1 = new ConsumerRecord<>(topic, 0, 0, "key1", 1);
        final var rec2 = new ConsumerRecord<>(topic, 0, 1, "key2", 2);
        addRecordAndSeek(topic, mockConsumer, List.of(rec1, rec2));

        consumer.poll(duration)
                .onComplete(ar -> {
                    ar.succeeded();
                    // Verify that the consumer has polled the records
                    var iter = ar.result().iterator();
                    assertEquals(rec1, iter.next());
                    assertEquals(rec2, iter.next());
                    assertFalse(iter.hasNext());

                    checkpoints.flag();
                })
                .onFailure(testContext::failNow);
    }

    @Test
    public void testCommitOffsets(VertxTestContext testContext) throws InterruptedException {
        // Test data
        final String topic = "test-topic";
        final int partition = 0;
        int offset = 123;
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new ConcurrentHashMap<>();
        offsetMap.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));

        // Commit offsets
        consumer.commit(offsetMap)
                .onComplete(testContext.succeeding(result -> {
                    // Verify that the offsets are committed successfully
                    assertEquals(offsetMap, result);
                    testContext.completeNow();
                }))
                .onFailure(testContext::failNow);
    }

    private void addRecordAndSeek(
            String topic, MockConsumer<String, Integer> mockConsumer, List<ConsumerRecord<String, Integer>> records) {
        // Mock consumers need to seek manually since they cannot automatically reset offsets
        mockConsumer.rebalance(Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(topic, 0), 0L);
        beginningOffsets.put(new TopicPartition(topic, 1), 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.seek(new TopicPartition(topic, 0), 0);
        for (ConsumerRecord<String, Integer> record : records) {
            mockConsumer.addRecord(record);
        }
    }
}
