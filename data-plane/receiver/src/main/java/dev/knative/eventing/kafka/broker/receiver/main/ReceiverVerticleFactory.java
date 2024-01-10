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
import dev.knative.eventing.kafka.broker.core.file.FileWatcher;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.receiver.IngressRequestHandler;
import dev.knative.eventing.kafka.broker.receiver.impl.IngressProducerReconcilableStore;
import dev.knative.eventing.kafka.broker.receiver.impl.ReceiverVerticle;
import dev.knative.eventing.kafka.broker.receiver.impl.StrictRequestToRecordMapper;
import dev.knative.eventing.kafka.broker.receiver.impl.handler.IngressRequestHandlerImpl;
import io.cloudevents.CloudEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Verticle;
import io.vertx.core.http.HttpServerOptions;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

class ReceiverVerticleFactory implements Supplier<Verticle>, AutoCloseable {

    private final ReceiverEnv env;
    private final Properties producerConfigs;
    private final HttpServerOptions httpServerOptions;
    private final HttpServerOptions httpsServerOptions;

    private final String secretVolumePath = "/etc/receiver-tls-secret";

    private final IngressRequestHandler ingressRequestHandler;

    private ReactiveProducerFactory<String, CloudEvent> kafkaProducerFactory;

    private final List<ReceiverVerticle> verticles;
    private final FileWatcher fileWatcher;

    ReceiverVerticleFactory(
            final ReceiverEnv env,
            final Properties producerConfigs,
            final MeterRegistry metricsRegistry,
            final HttpServerOptions httpServerOptions,
            final HttpServerOptions httpsServerOptions,
            final ReactiveProducerFactory<String, CloudEvent> kafkaProducerFactory)
            throws IOException {
        {
            this.env = env;
            this.producerConfigs = producerConfigs;
            this.httpServerOptions = httpServerOptions;
            this.httpsServerOptions = httpsServerOptions;
            this.ingressRequestHandler =
                    new IngressRequestHandlerImpl(StrictRequestToRecordMapper.getInstance(), metricsRegistry);
            this.kafkaProducerFactory = kafkaProducerFactory;
            this.verticles = new CopyOnWriteArrayList<>();

            this.fileWatcher = new FileWatcher(new File(this.secretVolumePath + "/tls.crt"), this::updateServerConfig);
        }
    }

    private void updateServerConfig() {
        for (final var v : this.verticles) {
            v.updateServerConfig();
        }
    }

    @Override
    public Verticle get() {
        final var verticle = new ReceiverVerticle(
                env,
                httpServerOptions,
                httpsServerOptions,
                v -> new IngressProducerReconcilableStore(
                        AuthProvider.kubernetes(),
                        producerConfigs,
                        properties -> kafkaProducerFactory.create(v, properties)),
                this.ingressRequestHandler,
                this.secretVolumePath);

        this.verticles.add(verticle);

        return verticle;
    }

    @Override
    public void close() throws Exception {
        this.fileWatcher.close();
    }
}
