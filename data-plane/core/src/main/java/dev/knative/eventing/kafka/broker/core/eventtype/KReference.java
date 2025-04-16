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
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import java.util.Objects;

@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "name", "namespace"})
public class KReference implements KubernetesResource {
    @JsonProperty("apiVersion")
    private String apiVersion;

    @JsonProperty("kind")
    private String kind;

    @JsonProperty("name")
    private String name;

    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("address")
    private String address;

    public KReference() {}

    public KReference(String apiVersion, String kind, String name, String namespace) {
        this.apiVersion = apiVersion;
        this.kind = kind;
        this.name = name;
        this.namespace = namespace;
    }

    public static KReference fromDataPlaneReference(DataPlaneContract.Reference reference) {
        return new KReference(
                reference.getGroupVersion(), reference.getKind(), reference.getName(), reference.getNamespace());
    }

    @JsonProperty("apiVersion")
    public String getApiVersion() {
        return this.apiVersion;
    }

    @JsonProperty("apiVersion")
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    @JsonProperty("kind")
    public String getKind() {
        return this.kind;
    }

    @JsonProperty("kind")
    public void setKind(String kind) {
        this.kind = kind;
    }

    @JsonProperty("name")
    public String getName() {
        return this.name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("namespace")
    public String getNamespace() {
        return this.namespace;
    }

    @JsonProperty("namespace")
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @JsonProperty("address")
    public String getAddress() {
        return this.address;
    }

    @JsonProperty("address")
    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KReference)) return false;
        KReference that = (KReference) o;
        return Objects.equals(this.getApiVersion(), that.getApiVersion())
                && Objects.equals(this.getKind(), that.getKind())
                && Objects.equals(this.getName(), that.getName())
                && Objects.equals(this.getNamespace(), that.getNamespace())
                && Objects.equals(this.getAddress(), that.getAddress());
    }

    protected boolean canEqual(Object o) {
        return o instanceof KReference;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.getApiVersion(), this.getKind(), this.getName(), this.getNamespace(), this.getAddress());
    }
}
