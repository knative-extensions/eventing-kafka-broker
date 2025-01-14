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
package dev.knative.eventing.kafka.broker.receiverloom;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class LoomKafkaProducerTest {

    private Vertx vertx;
    private LoomKafkaProducer<String, Integer> producer;

    @BeforeEach
    public void setUp() {
        vertx = Vertx.vertx();
        producer = new LoomKafkaProducer<>(vertx, mockProducer());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
    }

    @Test
    public void testConcurrentRecordSendCountAndSequence() throws InterruptedException {

        // Set up the test parameters
        final int numRecords = 100000;
        AtomicInteger counter = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(numRecords);
        List<Integer> receivedOrder = new ArrayList<>();

        // Send the records
        for (int i = 0; i < numRecords; i++) {
            int finalI = i;
            sendRecord(counter, latch, receivedOrder, finalI);
        }

        latch.await();

        // Verify the number of records sent
        assertEquals(numRecords, counter.get());

        // Verify the order of received records
        for (int i = 0; i < numRecords; i++) {
            assertEquals(i, receivedOrder.get(i));
        }
    }

    @Test
    public void testSendAfterClose(VertxTestContext testContext) throws ExecutionException, InterruptedException {

        // Close the producer before sending a record
        producer.close()
                .onFailure(testContext::failNow)
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        // Attempt to send a record after the producer is closed
        ProducerRecord<String, Integer> record = new ProducerRecord<>("test", "sequence number", 123);
        Future<RecordMetadata> sendFuture = producer.send(record);

        // Verify that the sendFuture fails with the expected error message
        sendFuture.onComplete(ar -> {
            testContext.verify(() -> {
                assertTrue(ar.failed());
                assertEquals("Producer is closed", ar.cause().getMessage());
                testContext.completeNow();
            });
        });
    }

    @Test
    public void testCloseIsWaitingForEmptyQueue(VertxTestContext testContext) {

        final int numRecords = 100000;
        final var checkpoints = testContext.checkpoint(numRecords + 1);
        // Send multiple records
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<String, Integer> record = new ProducerRecord<>("test", "sequence number", i);
            producer.send(record)
                    .onSuccess(ar -> {
                        checkpoints.flag();
                        assertTrue(producer.isSendFromQueueThreadAlive());
                    })
                    .onFailure(testContext::failNow);
        }

        // Close the producer and check that it is waiting for the queue to be empty
        producer.close()
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertFalse(producer.isSendFromQueueThreadAlive());

                        checkpoints.flag();
                    });
                })
                .onFailure(testContext::failNow);
    }

    @Test
    void interruptHappensWhenClose(VertxTestContext testContext) throws InterruptedException {

        final var checkpoints = testContext.checkpoint(2);
        // send a single record
        ProducerRecord<String, Integer> record = new ProducerRecord<>("test", "sequence number", 123);
        producer.send(record)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        assertTrue(ar.succeeded());
                        checkpoints.flag();
                    });
                })
                .onFailure(testContext::failNow);

        // now sendFromQueueThread should be blocked in eventQueue.take()

        // close the producer and verify the sendFromQueueThread is interrupted
        producer.close()
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        assertTrue(ar.succeeded());
                        assertFalse(producer.isSendFromQueueThreadAlive());
                        checkpoints.flag();
                    });
                })
                .onFailure(testContext::failNow);
    }

    @Test
    public void testFlush(VertxTestContext testContext) {

        final var checkpoints = testContext.checkpoint(2);
        // Send a record
        ProducerRecord<String, Integer> record = new ProducerRecord<>("test", "sequence number", 123);
        producer.send(record)
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        assertTrue(ar.succeeded());
                        checkpoints.flag();
                    });
                })
                .onFailure(testContext::failNow);

        // Flush the producer and wait for it to complete
        producer.flush()
                .onComplete(ar -> {
                    testContext.verify(() -> {
                        assertTrue(ar.succeeded());
                        checkpoints.flag();
                    });
                })
                .onFailure(testContext::failNow);
    }

    private void sendRecord(AtomicInteger counter, CountDownLatch latch, List<Integer> receivedOrder, int i) {
        ProducerRecord<String, Integer> record = new ProducerRecord<>("test", "sequence number", i);
        Future<RecordMetadata> sendFuture = producer.send(record);

        sendFuture.onComplete(ar -> {
            if (ar.succeeded()) {
                int index = counter.getAndIncrement();
                receivedOrder.add(index);
            }
            latch.countDown();
        });
    }

    @SuppressWarnings("unchecked")
    private static KafkaProducer<String, Integer> mockProducer() {
        KafkaProducer<String, Integer> producer = mock(KafkaProducer.class);
        when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);

            RecordMetadata metadata = new RecordMetadata(null, 0, 0, 0, -1, -1);

            callback.onCompletion(metadata, null);
            return null;
        });
        return producer;
    }
}
