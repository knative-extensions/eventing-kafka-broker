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

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.ReactiveKafkaProducer;
import dev.knative.eventing.kafka.broker.core.eventtype.EventType;
import dev.knative.eventing.kafka.broker.receiver.IngressProducer;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.AuthenticationException;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.AuthorizationException;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.EventPolicy;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.TokenVerifier;
import io.cloudevents.CloudEvent;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AuthHandlerTest {
    @Test
    public void shouldReturnUnauthorizedWhenJWTValidationFails() {
        final HttpServerRequest request = mock(HttpServerRequest.class);
        final var response = mockResponse(request, HttpResponseStatus.UNAUTHORIZED.code());

        TokenVerifier tokenVerifier = new TokenVerifier() {
            @Override
            public Future<Void> verify(HttpServerRequest request, IngressProducer ingressInfo) {
                return Future.failedFuture(new AuthenticationException("JWT validation failed"));
            }
        };

        final AuthHandler authHandler = new AuthHandler(tokenVerifier);

        authHandler.handle(
                request,
                new IngressProducer() {
                    @Override
                    public ReactiveKafkaProducer<String, CloudEvent> getKafkaProducer() {
                        return null;
                    }

                    @Override
                    public String getTopic() {
                        return null;
                    }

                    @Override
                    public DataPlaneContract.Reference getReference() {
                        return null;
                    }

                    @Override
                    public Lister<EventType> getEventTypeLister() {
                        return mock(Lister.class);
                    }

                    @Override
                    public String getAudience() {
                        return "some-required-audience";
                    }

                    @Override
                    public List<EventPolicy> getEventPolicies() {
                        return null;
                    }
                },
                mock(Handler.class));

        verify(response, times(1)).setStatusCode(HttpResponseStatus.UNAUTHORIZED.code());
        verify(response, times(1)).end();
    }

    @Test
    public void shouldReturnForbiddenWhenAuthorizationFails() {
        final HttpServerRequest request = mock(HttpServerRequest.class);
        final var response = mockResponse(request, HttpResponseStatus.FORBIDDEN.code());

        TokenVerifier tokenVerifier = new TokenVerifier() {
            @Override
            public Future<Void> verify(HttpServerRequest request, IngressProducer ingressInfo) {
                return Future.failedFuture(new AuthorizationException("AuthZ failed"));
            }
        };

        final AuthHandler authHandler = new AuthHandler(tokenVerifier);

        authHandler.handle(
                request,
                new IngressProducer() {
                    @Override
                    public ReactiveKafkaProducer<String, CloudEvent> getKafkaProducer() {
                        return null;
                    }

                    @Override
                    public String getTopic() {
                        return null;
                    }

                    @Override
                    public DataPlaneContract.Reference getReference() {
                        return null;
                    }

                    @Override
                    public Lister<EventType> getEventTypeLister() {
                        return mock(Lister.class);
                    }

                    @Override
                    public String getAudience() {
                        return "some-required-audience";
                    }

                    @Override
                    public List<EventPolicy> getEventPolicies() {
                        return null;
                    }
                },
                mock(Handler.class));

        verify(response, times(1)).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        verify(response, times(1)).end();
    }

    @Test
    public void shouldContinueWithRequestWhenJWTSucceeds() {
        final HttpServerRequest request = mock(HttpServerRequest.class);
        final var next = mock(Handler.class); // mockHandler(request);

        TokenVerifier tokenVerifier = new TokenVerifier() {
            @Override
            public Future<Void> verify(HttpServerRequest request, IngressProducer ingressInfo) {
                return Future.succeededFuture();
            }
        };

        final AuthHandler authHandler = new AuthHandler(tokenVerifier);

        authHandler.handle(
                request,
                new IngressProducer() {
                    @Override
                    public ReactiveKafkaProducer<String, CloudEvent> getKafkaProducer() {
                        return null;
                    }

                    @Override
                    public String getTopic() {
                        return null;
                    }

                    @Override
                    public DataPlaneContract.Reference getReference() {
                        return null;
                    }

                    @Override
                    public Lister<EventType> getEventTypeLister() {
                        return mock(Lister.class);
                    }

                    @Override
                    public String getAudience() {
                        return "some-required-audience";
                    }

                    @Override
                    public List<EventPolicy> getEventPolicies() {
                        return null;
                    }
                },
                next);

        verify(next, times(1)).handle(request);
    }

    private static HttpServerResponse mockResponse(final HttpServerRequest request, final int statusCode) {
        final var response = mock(HttpServerResponse.class);
        when(response.setStatusCode(statusCode)).thenReturn(response);
        when(request.response()).thenReturn(response);

        return response;
    }
}
