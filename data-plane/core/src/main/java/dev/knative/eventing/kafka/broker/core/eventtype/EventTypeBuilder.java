/*
 * Copyright © 2023 Knative Authors (knative-dev@googlegroups.com)
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

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;

public class EventTypeBuilder {

    private final EventTypeSpec spec;
    private final ObjectMetaBuilder objectMetaBuilder;

    public EventTypeBuilder() {
        this.spec = new EventTypeSpec();
        this.objectMetaBuilder = new ObjectMetaBuilder();
    }

    public EventType build() {
        var eventType = new EventType();
        eventType.setSpec(this.getSpec());
        eventType.setMetadata(this.objectMetaBuilder.build());
        return eventType;
    }

    private EventTypeSpec getSpec() {
        return this.spec;
    }

    public EventTypeBuilder withBroker(String broker) {
        this.spec.setBroker(broker);
        return this;
    }

    public EventTypeBuilder withReference(KReference reference) {
        this.spec.setReference(reference);
        return this;
    }

    public EventTypeBuilder withDescription(String description) {
        this.spec.setDescription(description);
        return this;
    }

    public EventTypeBuilder withSchema(String schema) {
        this.spec.setSchema(schema);
        return this;
    }

    public EventTypeBuilder withName(String name) {
        this.objectMetaBuilder.withName(name);
        return this;
    }

    public EventTypeBuilder withNamespace(String namespace) {
        this.objectMetaBuilder.withNamespace(namespace);
        return this;
    }

    public EventTypeBuilder withSchemaDescription(String schemaDescription) {
        this.spec.setSchemaDescription(schemaDescription);
        return this;
    }
}
