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
package dev.knative.eventing.kafka.broker.dispatcher.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.filter.Filter;
import dev.knative.eventing.kafka.broker.core.observability.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSenderMock;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcherListener;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandler;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandlerMock;
import dev.knative.eventing.kafka.broker.dispatcher.impl.consumer.InvalidCloudEvent;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;
import dev.knative.eventing.kafka.broker.dispatcher.main.FakeConsumerVerticleContext;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@ExtendWith(VertxExtension.class)
public class RecordDispatcherTest {

    private static final ConsumerVerticleContext resourceContext = FakeConsumerVerticleContext.get(
            FakeConsumerVerticleContext.get().getResource(),
            DataPlaneContract.Egress.newBuilder(
                            FakeConsumerVerticleContext.get().getEgress())
                    .setDestination("testdest")
                    .build());

    static {
        BackendRegistries.setupBackend(
                new MicrometerMetricsOptions()
                        .setMicrometerRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT))
                        .setRegistryName(Metrics.METRICS_REGISTRY_NAME),
                null);
    }

    private PrometheusMeterRegistry registry;

    @BeforeEach
    public void setUp() {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    @Test
    public void shouldNotSendToSubscriberNorToDeadLetterSinkIfValueDoesntMatch() {

        final RecordDispatcherListener receiver = offsetManagerMock();

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                value -> false,
                CloudEventSender.noop("subscriber send called"),
                CloudEventSender.noop("DLS send called"),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);

        final var record = record();
        dispatcherHandler.dispatch(record);

        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).recordDiscarded(record);
        verify(receiver, never()).successfullySentToSubscriber(any());
        verify(receiver, never()).successfullySentToDeadLetterSink(any());
        verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());

        assertNoEventDispatchLatency();
        assertEventProcessingLatency();
        assertNoDiscardedEventCount();
    }

    @Test
    public void shouldSendOnlyToSubscriberIfValueMatches() {

        final var sendCalled = new AtomicBoolean(false);
        final RecordDispatcherListener receiver = offsetManagerMock();

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                value -> true,
                new CloudEventSenderMock(record -> {
                    sendCalled.set(true);
                    simulateLatency(); // Simulate dispatch latency
                    return Future.succeededFuture();
                }),
                new CloudEventSenderMock(record -> {
                    fail("DLS send called");
                    return Future.succeededFuture();
                }),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);
        final var record = record();
        dispatcherHandler.dispatch(record);

        assertTrue(sendCalled.get());
        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).successfullySentToSubscriber(record);
        verify(receiver, never()).successfullySentToDeadLetterSink(any());
        verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());
        verify(receiver, never()).recordDiscarded(any());

        assertEventDispatchLatency();
        assertEventProcessingLatency();
        assertNoDiscardedEventCount();
    }

    @Test
    public void shouldSendToDeadLetterSinkIfValueMatchesAndSubscriberSenderFails() {

        final var subscriberSenderSendCalled = new AtomicBoolean(false);
        final var dlsSenderSendCalled = new AtomicBoolean(false);
        final RecordDispatcherListener receiver = offsetManagerMock();

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                value -> true,
                new CloudEventSenderMock(record -> {
                    subscriberSenderSendCalled.set(true);
                    simulateLatency(); // Simulate dispatch latency
                    return Future.failedFuture("");
                }),
                new CloudEventSenderMock(record -> {
                    dlsSenderSendCalled.set(true);
                    return Future.succeededFuture();
                }),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);
        final var record = record();
        dispatcherHandler.dispatch(record);

        assertTrue(subscriberSenderSendCalled.get());
        assertTrue(dlsSenderSendCalled.get());
        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).successfullySentToDeadLetterSink(any());
        verify(receiver, never()).successfullySentToSubscriber(any());
        verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());
        verify(receiver, never()).recordDiscarded(any());

        assertEventDispatchLatency();
        assertEventProcessingLatency();
        assertNoDiscardedEventCount();
    }

    @Test
    public void shouldCallFailedToSendToDeadLetterSinkIfValueMatchesAndSubscriberAndDeadLetterSinkSenderFail() {

        final var subscriberSenderSendCalled = new AtomicBoolean(false);
        final var dlsSenderSendCalled = new AtomicBoolean(false);
        final RecordDispatcherListener receiver = offsetManagerMock();

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                value -> true,
                new CloudEventSenderMock(record -> {
                    subscriberSenderSendCalled.set(true);
                    simulateLatency(); // Simulate dispatch latency
                    return Future.failedFuture("");
                }),
                new CloudEventSenderMock(record -> {
                    dlsSenderSendCalled.set(true);
                    return Future.failedFuture("");
                }),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);
        final var record = record();
        dispatcherHandler.dispatch(record);

        assertTrue(subscriberSenderSendCalled.get());
        assertTrue(dlsSenderSendCalled.get());
        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).failedToSendToDeadLetterSink(any(), any());
        verify(receiver, never()).successfullySentToDeadLetterSink(any());
        verify(receiver, never()).successfullySentToSubscriber(any());
        verify(receiver, never()).recordDiscarded(any());

        assertEventDispatchLatency();
        assertEventProcessingLatency();
        assertNoDiscardedEventCount();
    }

    @Test
    public void failedEventsShouldBeEnhancedWithErrorExtensionsPriorToSendingToDls() {

        final var subscriberSenderSendCalled = new AtomicBoolean(false);
        final var dlsSenderSendCalled = new AtomicBoolean(false);
        final RecordDispatcherListener receiver = offsetManagerMock();

        int errorCode = 422;
        String errorBody = "{ \"message\": \"bad bad things happened\" }";

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                value -> true,
                new CloudEventSenderMock(record -> {
                    subscriberSenderSendCalled.set(true);
                    return Future.failedFuture(
                            new ResponseFailureException(makeHttpResponse(errorCode, errorBody), ""));
                }),
                new CloudEventSenderMock(record -> {
                    dlsSenderSendCalled.set(true);
                    return Future.succeededFuture();
                }),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);
        final var record = record();
        dispatcherHandler.dispatch(record);

        ArgumentCaptor<ConsumerRecord<Object, CloudEvent>> captor = ArgumentCaptor.forClass(ConsumerRecord.class);

        assertTrue(subscriberSenderSendCalled.get());
        assertTrue(dlsSenderSendCalled.get());
        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).successfullySentToDeadLetterSink(captor.capture());
        verify(receiver, never()).successfullySentToSubscriber(any());
        verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());
        verify(receiver, never()).recordDiscarded(any());

        ConsumerRecord<Object, CloudEvent> failedRecord = captor.getValue();
        assertEquals(record.topic(), failedRecord.topic());
        assertEquals(record.partition(), failedRecord.partition());
        assertEquals(record.offset(), failedRecord.offset());
        assertEquals(record.key(), failedRecord.key());
        assertEquals(record.value().getId(), failedRecord.value().getId());
        assertEquals(record.value().getAttributeNames(), failedRecord.value().getAttributeNames());
        assertEquals(record.value().getData(), failedRecord.value().getData());
        assertEquals("testdest", failedRecord.value().getExtension("knativeerrordest"));
        assertEquals(String.valueOf(errorCode), failedRecord.value().getExtension("knativeerrorcode"));
        assertEquals(
                Base64.getEncoder().encodeToString(errorBody.getBytes()),
                failedRecord.value().getExtension("knativeerrordata"));

        assertEventDispatchLatency();
        assertEventProcessingLatency();
        assertNoDiscardedEventCount();
    }

    @Test
    public void failedEventsShouldBeEnhancedWithCustomHttpHeaders() {

        final var subscriberSenderSendCalled = new AtomicBoolean(false);
        final var dlsSenderSendCalled = new AtomicBoolean(false);
        final RecordDispatcherListener receiver = offsetManagerMock();

        int errorCode = 422;
        String errorBody = "{ \"message\": \"bad bad things happened\" }";
        String validErrorKey = "kne-testerror";
        String invalidErrorKey = "something";
        MultiMap headerMap =
                MultiMap.caseInsensitiveMultiMap().add(validErrorKey, "hello").add(invalidErrorKey, "nope");

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                value -> true,
                new CloudEventSenderMock(record -> {
                    subscriberSenderSendCalled.set(true);
                    return Future.failedFuture(new ResponseFailureException(
                            makeHttpResponseWithHeaders(errorCode, errorBody, headerMap), ""));
                }),
                new CloudEventSenderMock(record -> {
                    dlsSenderSendCalled.set(true);
                    return Future.succeededFuture();
                }),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);
        final var record = record();
        dispatcherHandler.dispatch(record);

        ArgumentCaptor<ConsumerRecord<Object, CloudEvent>> captor = ArgumentCaptor.forClass(ConsumerRecord.class);

        assertTrue(subscriberSenderSendCalled.get());
        assertTrue(dlsSenderSendCalled.get());
        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).successfullySentToDeadLetterSink(captor.capture());
        verify(receiver, never()).successfullySentToSubscriber(any());
        verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());
        verify(receiver, never()).recordDiscarded(any());

        ConsumerRecord<Object, CloudEvent> failedRecord = captor.getValue();
        assertEquals(record.topic(), failedRecord.topic());
        assertEquals(record.partition(), failedRecord.partition());
        assertEquals(record.offset(), failedRecord.offset());
        assertEquals(record.key(), failedRecord.key());
        assertEquals(record.value().getId(), failedRecord.value().getId());
        assertEquals(record.value().getAttributeNames(), failedRecord.value().getAttributeNames());
        assertEquals(record.value().getData(), failedRecord.value().getData());
        assertEquals("testdest", failedRecord.value().getExtension("knativeerrordest"));
        assertEquals(String.valueOf(errorCode), failedRecord.value().getExtension("knativeerrorcode"));
        assertEquals("hello", failedRecord.value().getExtension("testerror"));
        assertThat(failedRecord.value().getExtension("something")).isNull();
        assertEquals(
                Base64.getEncoder().encodeToString(errorBody.getBytes()),
                failedRecord.value().getExtension("knativeerrordata"));

        assertEventDispatchLatency();
        assertEventProcessingLatency();
        assertNoDiscardedEventCount();
    }

    @Test
    public void failedEventsShouldBeEnhancedWithErrorExtensionsPriorToSendingToDlsBodyTooLarge() {

        final var subscriberSenderSendCalled = new AtomicBoolean(false);
        final var dlsSenderSendCalled = new AtomicBoolean(false);
        final RecordDispatcherListener receiver = offsetManagerMock();

        int errorCode = 422;
        String errorBody = "A".repeat(1024);
        String errorBodyTooLarge = errorBody + "QWERTY";

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                value -> true,
                new CloudEventSenderMock(record -> {
                    subscriberSenderSendCalled.set(true);
                    return Future.failedFuture(
                            new ResponseFailureException(makeHttpResponse(errorCode, errorBodyTooLarge), ""));
                }),
                new CloudEventSenderMock(record -> {
                    dlsSenderSendCalled.set(true);
                    return Future.succeededFuture();
                }),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);
        final var record = record();
        dispatcherHandler.dispatch(record);

        ArgumentCaptor<ConsumerRecord<Object, CloudEvent>> captor = ArgumentCaptor.forClass(ConsumerRecord.class);

        assertTrue(subscriberSenderSendCalled.get());
        assertTrue(dlsSenderSendCalled.get());
        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).successfullySentToDeadLetterSink(captor.capture());
        verify(receiver, never()).successfullySentToSubscriber(any());
        verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());
        verify(receiver, never()).recordDiscarded(any());

        ConsumerRecord<Object, CloudEvent> failedRecord = captor.getValue();
        assertEquals(record.topic(), failedRecord.topic());
        assertEquals(record.partition(), failedRecord.partition());
        assertEquals(record.offset(), failedRecord.offset());
        assertEquals(record.key(), failedRecord.key());
        assertEquals(record.value().getId(), failedRecord.value().getId());
        assertEquals(record.value().getAttributeNames(), failedRecord.value().getAttributeNames());
        assertEquals(record.value().getData(), failedRecord.value().getData());
        assertEquals("testdest", failedRecord.value().getExtension("knativeerrordest"));
        assertEquals(String.valueOf(errorCode), failedRecord.value().getExtension("knativeerrorcode"));
        assertEquals(
                Base64.getEncoder().encodeToString(errorBody.getBytes()),
                failedRecord.value().getExtension("knativeerrordata"));

        assertEventDispatchLatency();
        assertEventProcessingLatency();
        assertNoDiscardedEventCount();
    }

    private HttpResponseImpl<Buffer> makeHttpResponse(int statusCode, String body) {
        return makeHttpResponseWithHeaders(statusCode, body, MultiMap.caseInsensitiveMultiMap());
    }

    private HttpResponseImpl<Buffer> makeHttpResponseWithHeaders(int statusCode, String body, MultiMap headers) {
        return new HttpResponseImpl<Buffer>(
                HttpVersion.HTTP_2,
                statusCode,
                "",
                headers,
                MultiMap.caseInsensitiveMultiMap(),
                Collections.emptyList(),
                Buffer.buffer(body, "UTF-8"),
                Collections.emptyList());
    }

    @Test
    public void
            shouldCallFailedToSendToDeadLetterSinkIfValueMatchesAndSubscriberSenderFailsAndNoDeadLetterSinkSender() {
        final var subscriberSenderSendCalled = new AtomicBoolean(false);
        final RecordDispatcherListener receiver = offsetManagerMock();

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                value -> true,
                new CloudEventSenderMock(record -> {
                    subscriberSenderSendCalled.set(true);
                    simulateLatency(); // Simulate dispatch latency
                    return Future.failedFuture("");
                }),
                CloudEventSender.noop("No DLS configured"),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);
        final var record = record();
        dispatcherHandler.dispatch(record);

        assertTrue(subscriberSenderSendCalled.get());
        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).failedToSendToDeadLetterSink(any(), any());
        verify(receiver, never()).successfullySentToDeadLetterSink(any());
        verify(receiver, never()).successfullySentToSubscriber(any());
        verify(receiver, never()).recordDiscarded(any());

        assertEventDispatchLatency();
        assertEventProcessingLatency();
        assertNoDiscardedEventCount();
    }

    @Test
    public void shouldCloseSinkResponseHandlerSubscriberSenderAndDeadLetterSinkSender(final VertxTestContext context) {

        final var subscriberSender = mock(CloudEventSender.class);
        when(subscriberSender.close()).thenReturn(Future.succeededFuture());

        final var sinkResponseHandler = mock(ResponseHandler.class);
        when(sinkResponseHandler.close()).thenReturn(Future.succeededFuture());

        final var deadLetterSender = mock(CloudEventSender.class);
        when(deadLetterSender.close()).thenReturn(Future.succeededFuture());

        final RecordDispatcher recordDispatcher = new RecordDispatcherImpl(
                resourceContext,
                Filter.noop(),
                subscriberSender,
                deadLetterSender,
                sinkResponseHandler,
                offsetManagerMock(),
                null,
                Metrics.getRegistry());

        recordDispatcher
                .close()
                .onFailure(context::failNow)
                .onSuccess(r -> context.verify(() -> {
                    verify(subscriberSender, times(1)).close();
                    verify(sinkResponseHandler, times(1)).close();
                    verify(deadLetterSender, times(1)).close();
                    context.completeNow();
                }));
    }

    @Test
    public void shouldDiscardRecordIfInvalidCloudEvent() {

        final RecordDispatcherListener receiver = offsetManagerMock();

        final var dispatcherHandler = new RecordDispatcherImpl(
                resourceContext,
                Filter.noop(),
                CloudEventSender.noop("subscriber send called"),
                CloudEventSender.noop("DLS send called"),
                new ResponseHandlerMock(),
                receiver,
                null,
                registry);

        final var record = invalidRecord();
        dispatcherHandler.dispatch(record);

        verify(receiver, times(1)).recordReceived(record);
        verify(receiver, times(1)).recordDiscarded(record);
        verify(receiver, never()).successfullySentToSubscriber(any());
        verify(receiver, never()).successfullySentToDeadLetterSink(any());
        verify(receiver, never()).failedToSendToDeadLetterSink(any(), any());

        assertDiscardedEventCount();
        assertNoEventDispatchLatency();
    }

    private static ConsumerRecord<Object, CloudEvent> record() {
        return new ConsumerRecord<>("", 0, 0L, "", CoreObjects.event());
    }

    private static ConsumerRecord<Object, CloudEvent> invalidRecord() {
        return new ConsumerRecord<>("", 0, 0L, "", new InvalidCloudEvent(new byte[] {1, 4}));
    }

    public static RecordDispatcherListener offsetManagerMock() {
        final var m = mock(RecordDispatcherListener.class);
        when(m.close()).thenReturn(Future.succeededFuture());
        return m;
    }

    private void assertNoEventDispatchLatency() {
        var dispatchLatencyNotFound = false;
        try {
            assertThat(registry.get(Metrics.EVENT_DISPATCH_LATENCY).summaries().stream()
                            .reduce((a, b) -> b)
                            .isEmpty())
                    .isTrue();
        } catch (MeterNotFoundException ignored) {
            dispatchLatencyNotFound = true;
        }
        assertThat(dispatchLatencyNotFound).isTrue();
    }

    private void assertEventDispatchLatency() {
        await().untilAsserted(() -> assertThat(registry.get(Metrics.EVENT_DISPATCH_LATENCY).summaries().stream()
                        .reduce((a, b) -> b)
                        .isPresent())
                .isTrue());
    }

    private void assertEventProcessingLatency() {
        await().untilAsserted(() -> assertThat(registry.get(Metrics.EVENT_PROCESSING_LATENCY).summaries().stream()
                        .reduce((a, b) -> b)
                        .isPresent())
                .isTrue());
    }

    private void assertNoDiscardedEventCount() {
        var counterNotFound = false;
        try {
            assertThat(registry.get(Metrics.DISCARDED_EVENTS_COUNT).counters().stream()
                            .reduce((a, b) -> b)
                            .isEmpty())
                    .isTrue();
        } catch (MeterNotFoundException ignored) {
            counterNotFound = true;
        }
        assertThat(counterNotFound).isTrue();
    }

    private void assertDiscardedEventCount() {
        assertThat(registry.get(Metrics.DISCARDED_EVENTS_COUNT).counters().stream()
                        .reduce((a, b) -> b)
                        .get()
                        .count())
                .isGreaterThan(0);
    }

    private void simulateLatency() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
    }
}
