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
import java.util.Objects;

@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"broker", "reference", "description", "schema", "schemaData"})
public class EventTypeSpec implements KubernetesResource {

    @JsonProperty("broker")
    private String broker;

    @JsonProperty("reference")
    private KReference reference;

    @JsonProperty("description")
    private String description;

    @JsonProperty("schema")
    private String schema;

    @JsonProperty("schemaDescription")
    private String schemaDescription;

    public EventTypeSpec() {}

    public EventTypeSpec(
            String broker, KReference reference, String description, String schema, String schemaDescription) {
        this.broker = broker;
        this.reference = reference;
        this.description = description;
        this.schema = schema;
        this.schemaDescription = schemaDescription;
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

    @JsonProperty("schema")
    public String getSchema() {
        return this.schema;
    }

    @JsonProperty("schema")
    public void setSchema(String schema) {
        this.schema = schema;
    }

    @JsonProperty("schemaDescription")
    public String getSchemaDescription() {
        return this.schemaDescription;
    }

    @JsonProperty("schemaDescription")
    public void setSchemaDescription(String schemaDescription) {
        this.schemaDescription = schemaDescription;
    }

    public String toString() {
        return "EventType{broker: " + this.getBroker() + ", " + "reference: "
                + this.getReference().toString() + ", description: " + this.getDescription() + ", schema: "
                + this.getSchema() + ", schemaDescription:" + this.getSchemaDescription();
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
                && Objects.equals(this.getSchemaDescription(), that.getSchemaDescription());
    }

    protected boolean canEqual(Object o) {
        return o instanceof EventTypeSpec;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.getBroker(),
                this.getReference(),
                this.getDescription(),
                this.getSchema(),
                this.getSchemaDescription());
    }
}
