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
package dev.knative.eventing.kafka.broker.receiver.impl.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.eventtype.EventType;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import dev.knative.eventing.kafka.broker.receiver.MockReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.receiver.RequestContext;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.EventPolicy;
import io.cloudevents.CloudEvent;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.List;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class IngressRequestHandlerImplTest {

    static {
        BackendRegistries.setupBackend(
                new MicrometerMetricsOptions()
                        .setMicrometerRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT))
                        .setRegistryName(Metrics.METRICS_REGISTRY_NAME),
                null);
    }

    @Test
    public void shouldSendRecordAndTerminateRequestWithRecordProduced() {
        shouldSendRecord(false, IngressRequestHandlerImpl.RECORD_PRODUCED);
    }

    @Test
    public void shouldSendRecordAndTerminateRequestWithFailedToProduce() {
        shouldSendRecord(true, IngressRequestHandlerImpl.FAILED_TO_PRODUCE);
    }

    private static void shouldSendRecord(boolean failedToSend, int statusCode) {
        final var cloudEvent = CoreObjects.event();
        final ReactiveKafkaProducer<String, CloudEvent> producer = mockProducer();

        when(producer.send(any())).thenAnswer(invocationOnMock -> {
            if (failedToSend) {
                return Future.failedFuture("failure");
            } else {
                return Future.succeededFuture(new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0));
            }
        });

        final HttpServerRequest request = mockHttpServerRequest("/hello");
        final var response = mockResponse(request, statusCode);

        final var handler = new IngressRequestHandlerImpl(Metrics.getRegistry(), ((event, lister, reference) -> null));

        handler.handle(new RequestContext(request), cloudEvent, new IngressProducer() {
            @Override
            public ReactiveKafkaProducer<String, CloudEvent> getKafkaProducer() {
                return producer;
            }

            @Override
            public String getTopic() {
                return "1-12345";
            }

            @Override
            public DataPlaneContract.Reference getReference() {
                return DataPlaneContract.Reference.newBuilder().build();
            }

            @Override
            public Lister<EventType> getEventTypeLister() {
                return mock(Lister.class);
            }

            @Override
            public String getAudience() {
                return "";
            }

            @Override
            public List<EventPolicy> getEventPolicies() {
                return null;
            }
        });

        verifySetStatusCodeAndTerminateResponse(statusCode, response);
    }

    @Test
    public void shouldReturnBadRequestIfNoCloudEvent() {
        final var producer = mockProducer();

        final HttpServerRequest request = mockHttpServerRequest("/hello");
        final var response = mockResponse(request, IngressRequestHandlerImpl.MAPPER_FAILED);

        final var handler = new IngressRequestHandlerImpl(Metrics.getRegistry(), ((event, lister, reference) -> null));

        handler.handle(new RequestContext(request), null, new IngressProducer() {
            @Override
            public ReactiveKafkaProducer<String, CloudEvent> getKafkaProducer() {
                return producer;
            }

            @Override
            public String getTopic() {
                return "1-12345";
            }

            @Override
            public DataPlaneContract.Reference getReference() {
                return DataPlaneContract.Reference.newBuilder().build();
            }

            @Override
            public Lister<EventType> getEventTypeLister() {
                return mock(Lister.class);
            }

            @Override
            public String getAudience() {
                return "";
            }

            @Override
            public List<EventPolicy> getEventPolicies() {
                return null;
            }
        });

        verifySetStatusCodeAndTerminateResponse(IngressRequestHandlerImpl.MAPPER_FAILED, response);
    }

    private static void verifySetStatusCodeAndTerminateResponse(
            final int statusCode, final HttpServerResponse response) {
        verify(response, times(1)).setStatusCode(statusCode);
        verify(response, times(1)).end();
    }

    @SuppressWarnings("unchecked")
    private static ReactiveKafkaProducer<String, CloudEvent> mockProducer() {
        ReactiveKafkaProducer<String, CloudEvent> producer = mock(MockReactiveKafkaProducer.class);
        when(producer.flush()).thenReturn(Future.succeededFuture());
        when(producer.close()).thenReturn(Future.succeededFuture());
        when(producer.unwrap()).thenReturn(new MockProducer<>());
        return producer;
    }

    private static HttpServerRequest mockHttpServerRequest(String path) {
        final var request = mock(HttpServerRequest.class);
        when(request.path()).thenReturn(path);
        when(request.method()).thenReturn(new HttpMethod("POST"));
        when(request.host()).thenReturn("127.0.0.1");
        when(request.scheme()).thenReturn("http");
        when(request.headers()).thenReturn(new HeadersMultiMap());
        return request;
    }

    private static HttpServerResponse mockResponse(final HttpServerRequest request, final int statusCode) {
        final var response = mock(HttpServerResponse.class);
        when(response.setStatusCode(statusCode)).thenReturn(response);
        when(request.response()).thenReturn(response);
        return response;
    }
}
