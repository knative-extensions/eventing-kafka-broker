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
import io.cloudevents.core.v1.CloudEventBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@EnableKubernetesMockClient(crud = true)
public class EventTypeCreatorImplTest {
    private KubernetesClient kubernetesClient;
    private KubernetesMockServer server;

    @Test
    public void testCreate() {
        final var eventTypeClient = kubernetesClient.resources(EventType.class);
        final var informer = eventTypeClient.inform();
        final var eventTypeLister = new Lister<>(informer.getIndexer());
        var eventTypeCreator = new EventTypeCreatorImpl(eventTypeClient, eventTypeLister);
        var event = new CloudEventBuilder()
                .withType("example.event.type")
                .withSource(URI.create("/example/source"))
                .withDataSchema(URI.create("/example/schema"))
                .withId("54321")
                .build();
        var reference = DataPlaneContract.Reference.newBuilder()
                .setNamespace("default")
                .setName("my-broker")
                .setKind("Broker")
                .setGroupVersion("eventing.knative.dev/v1")
                .setUuid("12345")
                .build();
        eventTypeCreator.create(event, reference);

        informer.close();

        KubernetesResourceList<EventType> eventTypeList =
                eventTypeClient.inNamespace("default").list();
        Assertions.assertNotNull(eventTypeList);
        Assertions.assertEquals(1, eventTypeList.getItems().size());
        var eventType = eventTypeList.getItems().get(0);
        Assertions.assertEquals(
                eventType.getSpec().getReference(),
                new KReference("eventing.knative.dev/v1", "Broker", "my-broker", "default"));
        Assertions.assertEquals(eventType.getSpec().getSchema(), URI.create("/example/schema"));
        Assertions.assertEquals(eventType.getSpec().getDescription(), "Event Type auto-created by controller");
        Assertions.assertEquals(
                eventType.getMetadata().getOwnerReferences().get(0),
                new OwnerReferenceBuilder()
                        .withApiVersion("eventing.knative.dev/v1")
                        .withKind("Broker")
                        .withName("my-broker")
                        .withUid("12345")
                        .build());
    }
}
