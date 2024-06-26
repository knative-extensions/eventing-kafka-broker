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
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;

public class EventTypeCreatorImpl implements EventTypeCreator {

    private static final Integer DNS1123_SUBDOMAIN_MAX_LENGTH = 253;

    private final MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient;

    private final EventTypeListerFactory eventTypeListerFactory;

    private MessageDigest messageDigest;

    private final WorkerExecutor executor;

    public EventTypeCreatorImpl(
            MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient,
            EventTypeListerFactory eventTypeListerFactory,
            Vertx vertx)
            throws IllegalArgumentException, NoSuchAlgorithmException {
        this.eventTypeClient = eventTypeClient;
        this.eventTypeListerFactory = eventTypeListerFactory;
        this.executor = vertx.createSharedWorkerExecutor("et-creator-worker", 1);
        this.messageDigest = MessageDigest.getInstance("MD5");
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

    private EventType eventTypeExists(String etName, DataPlaneContract.Reference reference) {
        return this.eventTypeListerFactory
                .getForNamespace(reference.getNamespace())
                .get(etName);
    }

    @Override
    public Future<EventType> create(CloudEvent event, DataPlaneContract.Reference ownerReference) {
        return this.executor.executeBlocking(() -> {
            final var name = this.getName(event, ownerReference);
            final var eventType = this.eventTypeExists(name, ownerReference);
            if (eventType != null) {
                return eventType;
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

            return this.eventTypeClient.resource(et.build()).create();
        });
    }
}
