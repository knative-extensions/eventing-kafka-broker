package dev.knative.eventing.kafka.broker.dispatcher;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

public class ConsumerRecordHandlerTest {

  @Test
  @SuppressWarnings("unchecked")
  public void shouldNotSendToSubscriberNorToDLQIfValueDoesntMatch() {

    final ConsumerRecordOffsetStrategy<Object, Object> receiver
        = (ConsumerRecordOffsetStrategy<Object, Object>) mock(ConsumerRecordOffsetStrategy.class);

    final var consumerRecordHandler = new ConsumerRecordHandler<>(
        record -> fail("subscriber send called"),
        value -> false,
        receiver,
        response -> Future.succeededFuture(),
        record -> fail("DLQ send called")
    );

    final var record = record();
    consumerRecordHandler.handle(record);

    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).recordDiscarded(record);
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).successfullySentToDLQ(any());
    verify(receiver, never()).failedToSendToDLQ(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSendOnlyToSubscriberIfValueMatches() {

    final var sendCalled = new AtomicBoolean(false);
    final ConsumerRecordOffsetStrategy<Object, Object> receiver
        = (ConsumerRecordOffsetStrategy<Object, Object>) mock(ConsumerRecordOffsetStrategy.class);

    final var consumerRecordHandler = new ConsumerRecordHandler<>(
        record -> {
          sendCalled.set(true);
          return Future.succeededFuture();
        },
        value -> true,
        receiver,
        response -> Future.succeededFuture(),
        record -> fail("DLQ send called")
    );
    final var record = record();
    consumerRecordHandler.handle(record);

    assertTrue(sendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).successfullySentToSubscriber(record);
    verify(receiver, never()).successfullySentToDLQ(any());
    verify(receiver, never()).failedToSendToDLQ(any(), any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSendToDLQIfValueMatchesAndSubscriberSenderFails() {

    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final var DLQSenderSendCalled = new AtomicBoolean(false);
    final ConsumerRecordOffsetStrategy<Object, Object> receiver
        = (ConsumerRecordOffsetStrategy<Object, Object>) mock(ConsumerRecordOffsetStrategy.class);

    final var consumerRecordHandler = new ConsumerRecordHandler<>(
        record -> {
          subscriberSenderSendCalled.set(true);
          return Future.failedFuture("");
        },
        value -> true,
        receiver,
        response -> Future.succeededFuture(),
        record -> {
          DLQSenderSendCalled.set(true);
          return Future.succeededFuture();
        }
    );
    final var record = record();
    consumerRecordHandler.handle(record);

    assertTrue(subscriberSenderSendCalled.get());
    assertTrue(DLQSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).successfullySentToDLQ(record);
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).failedToSendToDLQ(any(), any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCallFailedToSendToDLQIfValueMatchesAndSubscriberAndDLQSenderFail() {

    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final var DLQSenderSendCalled = new AtomicBoolean(false);
    final ConsumerRecordOffsetStrategy<Object, Object> receiver
        = (ConsumerRecordOffsetStrategy<Object, Object>) mock(ConsumerRecordOffsetStrategy.class);

    final var consumerRecordHandler = new ConsumerRecordHandler<>(
        record -> {
          subscriberSenderSendCalled.set(true);
          return Future.failedFuture("");
        },
        value -> true,
        receiver,
        response -> Future.succeededFuture(),
        record -> {
          DLQSenderSendCalled.set(true);
          return Future.failedFuture("");
        }
    );
    final var record = record();
    consumerRecordHandler.handle(record);

    assertTrue(subscriberSenderSendCalled.get());
    assertTrue(DLQSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).failedToSendToDLQ(eq(record), any());
    verify(receiver, never()).successfullySentToDLQ(any());
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCallFailedToSendToDLQIfValueMatchesAndSubscriberSenderFailsAndNoDLQSender() {
    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final ConsumerRecordOffsetStrategy<Object, Object> receiver
        = (ConsumerRecordOffsetStrategy<Object, Object>) mock(ConsumerRecordOffsetStrategy.class);

    final var consumerRecordHandler = new ConsumerRecordHandler<>(
        record -> {
          subscriberSenderSendCalled.set(true);
          return Future.failedFuture("");
        },
        value -> true,
        receiver,
        response -> Future.succeededFuture()
    );
    final var record = record();
    consumerRecordHandler.handle(record);

    assertTrue(subscriberSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).failedToSendToDLQ(eq(record), any());
    verify(receiver, never()).successfullySentToDLQ(any());
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).recordDiscarded(any());
  }

  private static KafkaConsumerRecord<Object, Object> record() {
    return new KafkaConsumerRecordImpl<>(new ConsumerRecord<>("", 0, 0L, "", ""));
  }
}