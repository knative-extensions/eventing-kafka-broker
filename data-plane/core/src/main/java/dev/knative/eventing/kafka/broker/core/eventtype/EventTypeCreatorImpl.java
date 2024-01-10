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

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.cloudevents.CloudEvent;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;

public class EventTypeCreatorImpl implements EventTypeCreator {

    private static final Integer DNS1123_SUBDOMAIN_MAX_LENGTH = 253;

    private final MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient;

    private MessageDigest messageDigest;

    public EventTypeCreatorImpl(KubernetesClient kubernetesClient) {
        this.eventTypeClient = kubernetesClient.resources(EventType.class);
        try {
            this.messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ignored) {
            this.messageDigest = null;
        }
    }

    private String getName(CloudEvent event, DataPlaneContract.Reference reference) {
        final var suffixString = event.getType() + event.getSource() + reference.getNamespace() + reference.getName();
        this.messageDigest.reset();
        this.messageDigest.update(suffixString.getBytes());
        final var suffix = Hex.encodeHexString(this.messageDigest.digest());
        final var name = String.format("et-%s-%s", reference.getName(), suffix).toLowerCase();
        if (name.length() > DNS1123_SUBDOMAIN_MAX_LENGTH) {
            return name.substring(0, DNS1123_SUBDOMAIN_MAX_LENGTH);
        }
        return name;
    }

    private boolean eventTypeExists(String etName, DataPlaneContract.Reference reference) {
        var et = this.eventTypeClient
                .inNamespace(reference.getNamespace())
                .withName(etName)
                .get();
        return et != null;
    }

    @Override
    public void create(CloudEvent event, DataPlaneContract.Reference ownerReference) {
        if (this.messageDigest == null) {
            return;
        }

        var name = this.getName(event, ownerReference);
        if (this.eventTypeExists(name, ownerReference)) {
            return;
        }

        var et = new EventTypeBuilder()
                .withReference(KReference.fromDataPlaneReference(ownerReference))
                .withOwnerReference(new OwnerReferenceBuilder()
                        .withName(ownerReference.getName())
                        .withKind(ownerReference.getKind())
                        .withApiVersion(ownerReference.getGroupVersion())
                        .withUid(ownerReference.getUuid())
                        .build())
                .withNamespace(ownerReference.getNamespace())
                .withName(name)
                .withType(event.getType())
                .withSource(event.getSource())
                .withSchema(event.getDataSchema())
                .withDescription("Event Type auto-created by controller");

        this.eventTypeClient.resource(et.build()).create();
    }
}
