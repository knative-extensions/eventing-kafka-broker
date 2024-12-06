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

package dev.knative.eventing.kafka.broker.core.eventtype;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EventTypeListerFactory implements AutoCloseable {
    private static final long INFORMER_RESYNC_MS = 30 * 1000;
    private final Map<String, Lister<EventType>> listerMap;
    private final List<SharedIndexInformer<EventType>> informers;
    private final MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient;

    public EventTypeListerFactory(
            MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient) {
        this.eventTypeClient = eventTypeClient;
        this.listerMap = new HashMap<>();
        this.informers = new LinkedList<>();
    }

    public Lister<EventType> getForNamespace(String namespace) {
        if (this.listerMap.containsKey(namespace)) {
            return this.listerMap.get(namespace);
        }
        return this.createListerForNamespace(namespace);
    }

    private Lister<EventType> createListerForNamespace(String namespace) {
        final var informer = this.eventTypeClient.inNamespace(namespace).runnableInformer(INFORMER_RESYNC_MS);
        informer.start();
        this.informers.add(informer);
        final var lister = new Lister<>(informer.getIndexer(), namespace);
        this.listerMap.put(namespace, lister);
        return lister;
    }

    @Override
    public void close() {
        this.informers.forEach(SharedIndexInformer::close);
    }
}
