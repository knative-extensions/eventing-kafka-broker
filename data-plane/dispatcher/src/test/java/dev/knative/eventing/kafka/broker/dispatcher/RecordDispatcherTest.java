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
package dev.knative.eventing.kafka.broker.dispatcher;

import dev.knative.eventing.kafka.broker.core.filter.Filter;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import dev.knative.eventing.kafka.broker.dispatcher.consumer.OffsetManager;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class RecordDispatcherTest {

  @Test
  public void shouldNotSendToSubscriberNorToDLQIfValueDoesntMatch() {

    final OffsetManager receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcher(
      value -> false,
      ConsumerRecordSender.create(Future.failedFuture("subscriber send called"), Future.succeededFuture()),
      ConsumerRecordSender.create(Future.failedFuture("DLQ send called"), Future.succeededFuture()),
      new SinkResponseHandlerMock(
        Future::succeededFuture,
        response -> Future.succeededFuture()
      ), receiver, null);

    final var record = record();
    dispatcherHandler.handle(record);

    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).recordDiscarded(record);
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).successfullySentToDLQ(any());
    verify(receiver, never()).failedToSendToDLQ(any(), any());
  }

  @Test
  public void shouldSendOnlyToSubscriberIfValueMatches() {

    final var sendCalled = new AtomicBoolean(false);
    final OffsetManager receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcher(
      value -> true, new ConsumerRecordSenderMock(
      Future::succeededFuture,
      record -> {
        sendCalled.set(true);
        return Future.succeededFuture();
      }
    ),
      new ConsumerRecordSenderMock(
        Future::succeededFuture,
        record -> {
          fail("DLQ send called");
          return Future.succeededFuture();
        }
      ), new SinkResponseHandlerMock(
      Future::succeededFuture,
      response -> Future.succeededFuture()
    ), receiver, null);
    final var record = record();
    dispatcherHandler.handle(record);

    assertTrue(sendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).successfullySentToSubscriber(record);
    verify(receiver, never()).successfullySentToDLQ(any());
    verify(receiver, never()).failedToSendToDLQ(any(), any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  public void shouldSendToDLQIfValueMatchesAndSubscriberSenderFails() {

    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final var DLQSenderSendCalled = new AtomicBoolean(false);
    final OffsetManager receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcher(
      value -> true, new ConsumerRecordSenderMock(
      Future::succeededFuture,
      record -> {
        subscriberSenderSendCalled.set(true);
        return Future.failedFuture("");
      }
    ),
      new ConsumerRecordSenderMock(
        Future::succeededFuture,
        record -> {
          DLQSenderSendCalled.set(true);
          return Future.succeededFuture();
        }
      ), new SinkResponseHandlerMock(
      Future::succeededFuture,
      response -> Future.succeededFuture()
    ), receiver, null);
    final var record = record();
    dispatcherHandler.handle(record);

    assertTrue(subscriberSenderSendCalled.get());
    assertTrue(DLQSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).successfullySentToDLQ(record);
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).failedToSendToDLQ(any(), any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  public void shouldCallFailedToSendToDLQIfValueMatchesAndSubscriberAndDLQSenderFail() {

    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final var DLQSenderSendCalled = new AtomicBoolean(false);
    final OffsetManager receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcher(
      value -> true, new ConsumerRecordSenderMock(
      Future::succeededFuture,
      record -> {
        subscriberSenderSendCalled.set(true);
        return Future.failedFuture("");
      }
    ),
      new ConsumerRecordSenderMock(
        Future::succeededFuture,
        record -> {
          DLQSenderSendCalled.set(true);
          return Future.failedFuture("");
        }
      ), new SinkResponseHandlerMock(
      Future::succeededFuture,
      response -> Future.succeededFuture()
    ), receiver, null);
    final var record = record();
    dispatcherHandler.handle(record);

    assertTrue(subscriberSenderSendCalled.get());
    assertTrue(DLQSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).failedToSendToDLQ(eq(record), any());
    verify(receiver, never()).successfullySentToDLQ(any());
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  public void shouldCallFailedToSendToDLQIfValueMatchesAndSubscriberSenderFailsAndNoDLQSender() {
    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final OffsetManager receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcher(
      value -> true,
      new ConsumerRecordSenderMock(
        Future::succeededFuture,
        record -> {
          subscriberSenderSendCalled.set(true);
          return Future.failedFuture("");
        }
      ),
      ConsumerRecordSender.create(Future.failedFuture("No DLQ configured"), Future.succeededFuture()),
      new SinkResponseHandlerMock(
        Future::succeededFuture,
        response -> Future.succeededFuture()
      ),
      receiver, null);
    final var record = record();
    dispatcherHandler.handle(record);

    assertTrue(subscriberSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).failedToSendToDLQ(eq(record), any());
    verify(receiver, never()).successfullySentToDLQ(any());
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  public void shouldCloseSinkResponseHandlerSubscriberSenderAndDLSSender(final VertxTestContext context) {

    final var subscriberSender = mock(ConsumerRecordSender.class);
    when(subscriberSender.close()).thenReturn(Future.succeededFuture());

    final var sinkResponseHandler = mock(SinkResponseHandler.class);
    when(sinkResponseHandler.close()).thenReturn(Future.succeededFuture());

    final var deadLetterSender = mock(ConsumerRecordSender.class);
    when(deadLetterSender.close()).thenReturn(Future.succeededFuture());

    final RecordDispatcher recordDispatcher = new RecordDispatcher(
      Filter.noop(), subscriberSender,
      deadLetterSender, sinkResponseHandler, offsetManagerMock(), null);

    recordDispatcher.close()
      .onFailure(context::failNow)
      .onSuccess(r -> context.verify(() -> {
        verify(subscriberSender, times(1)).close();
        verify(sinkResponseHandler, times(1)).close();
        verify(deadLetterSender, times(1)).close();
        context.completeNow();
      }));
  }

  private static KafkaConsumerRecord<String, CloudEvent> record() {
    return new KafkaConsumerRecordImpl<>(new ConsumerRecord<>("", 0, 0L, "", CoreObjects.event()));
  }

  public static OffsetManager offsetManagerMock() {
    final OffsetManager offsetManager = mock(OffsetManager.class);

    when(offsetManager.recordReceived(any())).thenReturn(Future.succeededFuture());
    when(offsetManager.recordDiscarded(any())).thenReturn(Future.succeededFuture());
    when(offsetManager.successfullySentToDLQ(any())).thenReturn(Future.succeededFuture());
    when(offsetManager.successfullySentToSubscriber(any())).thenReturn(Future.succeededFuture());
    when(offsetManager.failedToSendToDLQ(any(), any())).thenReturn(Future.succeededFuture());

    return offsetManager;
  }
}
