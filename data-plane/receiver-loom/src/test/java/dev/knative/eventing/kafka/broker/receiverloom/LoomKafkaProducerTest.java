package dev.knative.eventing.kafka.broker.receiverloom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class LoomKafkaProducerTest {

    LoomKafkaProducer<String, Integer> producer;

    @BeforeEach
    public void setUp() {
        producer = new LoomKafkaProducer<>(mockProducer());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
    }

    @Test
    public void testConcurrentRecordSendCountAndSequence() throws InterruptedException {

        // Set up the test parameters
        int numRecords = 10000;
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
        when(producer.send(any(ProducerRecord.class))).thenReturn((sendDemoRecord()));
        return producer;
    }

    private static java.util.concurrent.Future<RecordMetadata> sendDemoRecord() {
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        RecordMetadata metadata = new RecordMetadata(null, 0, 0, 0, -1, -1);
        
        // Complete the future with the demo metadata
        future.complete(metadata);
        return future;
    }
}
