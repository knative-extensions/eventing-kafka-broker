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
package dev.knative.eventing.kafka.broker.dispatcher.main;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.observability.metrics.Metrics;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressContext;
import dev.knative.eventing.kafka.broker.core.security.AuthProvider;
import dev.knative.eventing.kafka.broker.core.testing.CoreObjects;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.HashMap;
import java.util.Set;

public class FakeConsumerVerticleContext {

    public static ConsumerVerticleContext get() {
        return new ConsumerVerticleContext()
                .withProducerConfigs(new HashMap<>())
                .withConsumerConfigs(new HashMap<>())
                .withMeterRegistry(Metrics.getRegistry())
                .withResource(new EgressContext(CoreObjects.resource1(), CoreObjects.egress1(), Set.of()));
    }

    public static ConsumerVerticleContext get(
            final DataPlaneContract.Resource resource, final DataPlaneContract.Egress egress) {
        return new ConsumerVerticleContext()
                .withProducerConfigs(new HashMap<>())
                .withConsumerConfigs(new HashMap<>())
                .withAuthProvider(AuthProvider.noAuth())
                .withWebClientOptions(new WebClientOptions())
                .withMeterRegistry(Metrics.getRegistry())
                .withResource(new EgressContext(resource, egress, Set.of()));
    }
}
