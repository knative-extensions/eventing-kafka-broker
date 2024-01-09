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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import java.net.URI;
import java.util.Objects;

@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "source", "schema", "schemaData", "broker", "reference", "description"})
public class EventTypeSpec implements KubernetesResource {

    @JsonProperty("type")
    private String type;

    @JsonProperty("source")
    private URI source;

    @JsonProperty("broker")
    private String broker;

    @JsonProperty("reference")
    private KReference reference;

    @JsonProperty("description")
    private String description;

    @JsonProperty("schema")
    private URI schema;

    @JsonProperty("schemaData")
    private String schemaData;

    public EventTypeSpec() {}

    public EventTypeSpec(
            String type,
            URI source,
            URI schema,
            String schemaData,
            String broker,
            KReference reference,
            String description) {
        this.type = type;
        this.source = source;
        this.schema = schema;
        this.schemaData = schemaData;
        this.broker = broker;
        this.reference = reference;
        this.description = description;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("type")
    public String getType() {
        return this.type;
    }

    @JsonProperty("source")
    public void setSource(URI source) {
        this.source = source;
    }

    @JsonProperty("source")
    public URI getSource() {
        return this.source;
    }

    @JsonProperty("schema")
    public URI getSchema() {
        return this.schema;
    }

    @JsonProperty("schema")
    public void setSchema(URI schema) {
        this.schema = schema;
    }

    @JsonProperty("schemaData")
    public String getSchemaData() {
        return this.schemaData;
    }

    @JsonProperty("schemaData")
    public void setSchemaData(String schemaData) {
        this.schemaData = schemaData;
    }

    @JsonProperty("broker")
    public String getBroker() {
        return this.broker;
    }

    @JsonProperty("broker")
    public void setBroker(String broker) {
        this.broker = broker;
    }

    @JsonProperty("reference")
    public KReference getReference() {
        return this.reference;
    }

    @JsonProperty("reference")
    public void setReference(KReference reference) {
        this.reference = reference;
    }

    @JsonProperty("description")
    public String getDescription() {
        return this.description;
    }

    @JsonProperty("description")
    public void setDescription(String description) {
        this.description = description;
    }

    public String toString() {
        return "EventType{broker: " + this.getBroker() + ", reference: "
                + this.getReference().toString() + ", description: " + this.getDescription() + ", schema: "
                + this.getSchema().toString() + ", schemaData: " + this.getSchemaData() + ", type: " + this.getType()
                + ", source: " + this.getSource().toString() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventTypeSpec)) return false;
        EventTypeSpec that = (EventTypeSpec) o;
        return Objects.equals(this.getBroker(), that.getBroker())
                && Objects.equals(this.getReference(), that.getReference())
                && Objects.equals(this.getDescription(), that.getDescription())
                && Objects.equals(this.getSchema(), that.getSchema())
                && Objects.equals(this.getSchemaData(), that.getSchemaData());
    }

    protected boolean canEqual(Object o) {
        return o instanceof EventTypeSpec;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.getBroker(), this.getReference(), this.getDescription(), this.getSchema(), this.getSchemaData());
    }
}
