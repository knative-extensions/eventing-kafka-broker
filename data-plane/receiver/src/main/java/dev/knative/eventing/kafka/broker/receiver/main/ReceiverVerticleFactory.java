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
package dev.knative.eventing.kafka.broker.receiver.main;

import dev.knative.eventing.kafka.broker.core.ReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.core.eventtype.EventType;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeCreatorImpl;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeListerFactory;
import dev.knative.eventing.kafka.broker.core.oidc.OIDCDiscoveryConfig;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.receiver.IngressRequestHandler;
import dev.knative.eventing.kafka.broker.receiver.impl.IngressProducerReconcilableStore;
import dev.knative.eventing.kafka.broker.receiver.impl.ReceiverVerticle;
import dev.knative.eventing.kafka.broker.receiver.impl.StrictRequestToRecordMapper;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.IngressRequestHandlerImpl;
import io.cloudevents.CloudEvent;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.function.Supplier;

class ReceiverVerticleFactory implements Supplier<Verticle> {

    private final ReceiverEnv env;
    private final Properties producerConfigs;
    private final HttpServerOptions httpServerOptions;
    private final HttpServerOptions httpsServerOptions;

    private final String secretVolumePath = "/etc/receiver-tls-secret";

    private final IngressRequestHandler ingressRequestHandler;
    private final OIDCDiscoveryConfig oidcDiscoveryConfig;
    private final EventTypeListerFactory eventTypeListerFactory;

    private ReactiveProducerFactory<String, CloudEvent> kafkaProducerFactory;

    ReceiverVerticleFactory(
            final ReceiverEnv env,
            final Properties producerConfigs,
            final MeterRegistry metricsRegistry,
            final HttpServerOptions httpServerOptions,
            final HttpServerOptions httpsServerOptions,
            final ReactiveProducerFactory<String, CloudEvent> kafkaProducerFactory,
            final MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient,
            Vertx vertx,
            final OIDCDiscoveryConfig oidcDiscoveryConfig,
            final EventTypeListerFactory eventTypeListerFactory)
            throws NoSuchAlgorithmException {
        {
            this.env = env;
            this.producerConfigs = producerConfigs;
            this.httpServerOptions = httpServerOptions;
            this.httpsServerOptions = httpsServerOptions;
            this.ingressRequestHandler = new IngressRequestHandlerImpl(
                    StrictRequestToRecordMapper.getInstance(),
                    metricsRegistry,
                    new EventTypeCreatorImpl(eventTypeClient, vertx));
            this.kafkaProducerFactory = kafkaProducerFactory;
            this.oidcDiscoveryConfig = oidcDiscoveryConfig;
            this.eventTypeListerFactory = eventTypeListerFactory;
        }
    }

    @Override
    public Verticle get() {
        return new ReceiverVerticle(
                env,
                httpServerOptions,
                httpsServerOptions,
                v -> new IngressProducerReconcilableStore(
                        AuthProvider.kubernetes(v),
                        producerConfigs,
                        properties -> kafkaProducerFactory.create(v, properties),
                        eventTypeListerFactory),
                this.ingressRequestHandler,
                secretVolumePath,
                oidcDiscoveryConfig);
    }
}
