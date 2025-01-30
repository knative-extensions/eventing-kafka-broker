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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import dev.knative.eventing.kafka.broker.core.eventtype.EventType;
import dev.knative.eventing.kafka.broker.core.eventtype.EventTypeListerFactory;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import dev.knative.eventing.kafka.broker.receiver.MockReactiveProducerFactory;
import dev.knative.eventing.kafka.broker.receiver.impl.auth.OIDCDiscoveryConfigListener;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class ReceiverVerticleFactoryTest {

    static {
        BackendRegistries.setupBackend(
                new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME), null);
    }

    @Test
    public void shouldCreateMultipleReceiverVerticleInstances(Vertx vertx) throws NoSuchAlgorithmException {
        var mockClient = mock(KubernetesClient.class);
        final var supplier = new ReceiverVerticleFactory(
                mock(ReceiverEnv.class),
                mock(Properties.class),
                mock(MeterRegistry.class),
                mock(HttpServerOptions.class),
                mock(HttpServerOptions.class),
                mock(MockReactiveProducerFactory.class),
                mockClient.resources(EventType.class),
                vertx,
                mock(OIDCDiscoveryConfigListener.class),
                mock(EventTypeListerFactory.class));

        assertThat(supplier.get()).isNotSameAs(supplier.get());
    }
}
