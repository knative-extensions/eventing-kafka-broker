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

import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import java.util.HashMap;
import java.util.Map;

public class EventTypeListerFactory {
    private final Map<String, Lister<EventType>> listerMap;
    private final SharedIndexInformer<EventType> eventTypeInformer;

    public EventTypeListerFactory(SharedIndexInformer<EventType> eventTypeInformer) {
        if (eventTypeInformer == null) {
            throw new IllegalArgumentException("you must provide a non null eventtype informer");
        }
        this.eventTypeInformer = eventTypeInformer;
        this.listerMap = new HashMap<>();
    }

    public Lister<EventType> getForNamespace(String namespace) {
        if (this.listerMap.containsKey(namespace)) {
            return this.listerMap.get(namespace);
        }
        return this.createListerForNamespace(namespace);
    }

    private Lister<EventType> createListerForNamespace(String namespace) {
        final var lister = new Lister<>(this.eventTypeInformer.getIndexer(), namespace);
        this.listerMap.put(namespace, lister);
        return lister;
    }
}
