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
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;

@EnableKubernetesMockClient(crud = true)
public class EventTypeTest {

    private KubernetesClient kubernetesClient;
    private KubernetesMockServer server;

    @Test
    public void testCreateEventType() {
        var eventTypeClient = kubernetesClient.resources(EventType.class);
        OwnerReference ownerReference = new OwnerReferenceBuilder()
                .withName("MyBroker")
                .withKind("KafkaBroker")
                .withApiVersion("eventing.knative.dev/v1")
                .build();
        KReference kReference = new KReference("eventing.knative.dev/v1", "KafkaBroker", "MyBroker", "default");
        eventTypeClient
                .resource(new EventTypeBuilder()
                        .withReference(kReference)
                        .withSchema(URI.create("/sample/schema"))
                        .withSchemaData("sample schema data")
                        .withDescription("a sample event type")
                        .withName("sample.event.type")
                        .withNamespace("default")
                        .withOwnerReference(ownerReference)
                        .build())
                .create();

        KubernetesResourceList<EventType> eventTypeList =
                eventTypeClient.inNamespace("default").list();
        Assertions.assertNotNull(eventTypeList);
        Assertions.assertEquals(1, eventTypeList.getItems().size());
        EventType eventType = eventTypeList.getItems().get(0);
        Assertions.assertEquals(eventType.getSpec().getReference(), kReference);
        Assertions.assertEquals(eventType.getSpec().getSchema(), URI.create("/sample/schema"));
        Assertions.assertEquals(eventType.getSpec().getSchemaData(), "sample schema data");
        Assertions.assertEquals(eventType.getSpec().getDescription(), "a sample event type");
        Assertions.assertEquals(eventType.getMetadata().getName(), "sample.event.type");
        Assertions.assertEquals(eventType.getMetadata().getNamespace(), "default");
        Assertions.assertEquals(eventType.getMetadata().getOwnerReferences().get(0), ownerReference);
    }
}
