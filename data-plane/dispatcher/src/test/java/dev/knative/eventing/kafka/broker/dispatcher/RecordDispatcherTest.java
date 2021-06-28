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

import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
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
  public void shouldNotSendToSubscriberNorToDeadLetterSinkIfValueDoesntMatch() {

    final RecordDispatcherListener receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcherImpl(
      value -> false,
      ConsumerRecordSender.noop("subscriber send called"),
      ConsumerRecordSender.noop("DLS send called"),
      new SinkResponseHandlerMock(),
      receiver,
      null
    );

    final var record = record();
    dispatcherHandler.dispatch(record);

    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).recordDiscarded(record);
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).successfullySentToDeadLetterSink(any());
    verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());
  }

  @Test
  public void shouldSendOnlyToSubscriberIfValueMatches() {

    final var sendCalled = new AtomicBoolean(false);
    final RecordDispatcherListener receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcherImpl(
      value -> true, new ConsumerRecordSenderMock(
      record -> {
        sendCalled.set(true);
        return Future.succeededFuture();
      }
    ),
      new ConsumerRecordSenderMock(
        record -> {
          fail("DLS send called");
          return Future.succeededFuture();
        }
      ),
      new SinkResponseHandlerMock(),
      receiver,
      null
    );
    final var record = record();
    dispatcherHandler.dispatch(record);

    assertTrue(sendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).successfullySentToSubscriber(record);
    verify(receiver, never()).successfullySentToDeadLetterSink(any());
    verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  public void shouldSendToDeadLetterSinkIfValueMatchesAndSubscriberSenderFails() {

    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final var dlsSenderSendCalled = new AtomicBoolean(false);
    final RecordDispatcherListener receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcherImpl(
      value -> true, new ConsumerRecordSenderMock(
      record -> {
        subscriberSenderSendCalled.set(true);
        return Future.failedFuture("");
      }
    ),
      new ConsumerRecordSenderMock(
        record -> {
          dlsSenderSendCalled.set(true);
          return Future.succeededFuture();
        }
      ), new SinkResponseHandlerMock(),
      receiver,
      null
    );
    final var record = record();
    dispatcherHandler.dispatch(record);

    assertTrue(subscriberSenderSendCalled.get());
    assertTrue(dlsSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).successfullySentToDeadLetterSink(record);
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  public void shouldCallFailedToSendToDeadLetterSinkIfValueMatchesAndSubscriberAndDeadLetterSinkSenderFail() {

    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final var dlsSenderSendCalled = new AtomicBoolean(false);
    final RecordDispatcherListener receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcherImpl(
      value -> true, new ConsumerRecordSenderMock(
      record -> {
        subscriberSenderSendCalled.set(true);
        return Future.failedFuture("");
      }
    ),
      new ConsumerRecordSenderMock(
        record -> {
          dlsSenderSendCalled.set(true);
          return Future.failedFuture("");
        }
      ),
      new SinkResponseHandlerMock(),
      receiver,
      null
    );
    final var record = record();
    dispatcherHandler.dispatch(record);

    assertTrue(subscriberSenderSendCalled.get());
    assertTrue(dlsSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).failedToSendToDeadLetterSink(eq(record), any());
    verify(receiver, never()).successfullySentToDeadLetterSink(any());
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  public void shouldCallFailedToSendToDeadLetterSinkIfValueMatchesAndSubscriberSenderFailsAndNoDeadLetterSinkSender() {
    final var subscriberSenderSendCalled = new AtomicBoolean(false);
    final RecordDispatcherListener receiver = offsetManagerMock();

    final var dispatcherHandler = new RecordDispatcherImpl(
      value -> true,
      new ConsumerRecordSenderMock(
        record -> {
          subscriberSenderSendCalled.set(true);
          return Future.failedFuture("");
        }
      ),
      ConsumerRecordSender.noop("No DLS configured"),
      new SinkResponseHandlerMock(),
      receiver,
      null
    );
    final var record = record();
    dispatcherHandler.dispatch(record);

    assertTrue(subscriberSenderSendCalled.get());
    verify(receiver, times(1)).recordReceived(record);
    verify(receiver, times(1)).failedToSendToDeadLetterSink(eq(record), any());
    verify(receiver, never()).successfullySentToDeadLetterSink(any());
    verify(receiver, never()).successfullySentToSubscriber(any());
    verify(receiver, never()).recordDiscarded(any());
  }

  @Test
  public void shouldCloseSinkResponseHandlerSubscriberSenderAndDeadLetterSinkSender(final VertxTestContext context) {

    final var subscriberSender = mock(ConsumerRecordSender.class);
    when(subscriberSender.close()).thenReturn(Future.succeededFuture());

    final var sinkResponseHandler = mock(SinkResponseHandler.class);
    when(sinkResponseHandler.close()).thenReturn(Future.succeededFuture());

    final var deadLetterSender = mock(ConsumerRecordSender.class);
    when(deadLetterSender.close()).thenReturn(Future.succeededFuture());

    final RecordDispatcher recordDispatcher = new RecordDispatcherImpl(
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

  public static RecordDispatcherListener offsetManagerMock() {
    final RecordDispatcherListener recordDispatcherListener = mock(RecordDispatcherListener.class);

    when(recordDispatcherListener.recordReceived(any())).thenReturn(Future.succeededFuture());
    when(recordDispatcherListener.recordDiscarded(any())).thenReturn(Future.succeededFuture());
    when(recordDispatcherListener.successfullySentToDeadLetterSink(any())).thenReturn(Future.succeededFuture());
    when(recordDispatcherListener.successfullySentToSubscriber(any())).thenReturn(Future.succeededFuture());
    when(recordDispatcherListener.failedToSendToDeadLetterSink(any(), any())).thenReturn(Future.succeededFuture());

    return recordDispatcherListener;
  }
}
