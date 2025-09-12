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
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventTypeCreatorImpl implements EventTypeCreator {

    private static final Integer DNS1123_SUBDOMAIN_MAX_LENGTH = 253;

    private static final Logger logger = LoggerFactory.getLogger(EventTypeCreatorImpl.class);

    private final MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient;

    private MessageDigest messageDigest;

    private final WorkerExecutor executor;

    public EventTypeCreatorImpl(
            MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient,
            Vertx vertx)
            throws IllegalArgumentException, NoSuchAlgorithmException {
        this.eventTypeClient = eventTypeClient;
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

    @Override
    public Future<EventType> create(
            CloudEvent event, Lister<EventType> eventTypeLister, DataPlaneContract.Reference ownerReference) {
        return this.executor.executeBlocking(() -> {
            final var name = this.getName(event, ownerReference);
            logger.debug("attempting to autocreate eventtype {} for {}", name, ownerReference);
            final var eventType = eventTypeLister.get(name);
            if (eventType != null) {
                logger.debug("eventtype already exists");
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

            try {
                return this.eventTypeClient.resource(et.build()).create();
            } catch (io.fabric8.kubernetes.client.KubernetesClientException e) {
                // Handle race condition where EventType was created between cache check and creation attempt
                if ("AlreadyExists".equals(e.getStatus() != null ? e.getStatus().getReason() : null)) {
                    logger.debug("EventType {} already exists (race condition), fetching existing resource", name);
                    // Fetch the existing EventType instead of failing
                    final var existing = this.eventTypeClient.inNamespace(ownerReference.getNamespace()).withName(name).get();
                    if (existing != null) {
                        return existing;
                    }
                    // If we can't fetch it, fall back to the original exception
                    logger.warn("EventType {} already exists but could not fetch it, falling back to original error", name);
                }
                // Re-throw the original exception if it's not an AlreadyExists error or we couldn't handle it
                throw e;
            }
        });
    }
}
