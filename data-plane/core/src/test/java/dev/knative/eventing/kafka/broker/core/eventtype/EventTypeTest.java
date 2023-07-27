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

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@EnableKubernetesMockClient(crud = true)
public class EventTypeTest {

  static KubernetesClient client;
  MixedOperation<EventType, KubernetesResourceList<EventType>, Resource<EventType>> eventTypeClient;


  @Test
  public void testCreateEventType() {
    eventTypeClient
      .resource(
        new EventTypeBuilder()
          .withReference(new KReference("eventing.knative.dev/v1", "KafkaBroker", "MyBroker", "default"))
          .withSchema("sample schema")
          .withSchemaDescription("sample schema description")
          .withDescription("a sample event type")
          .withName("sample.event.type")
          .withNamespace("default")
          .build()
      ).create();

    KubernetesResourceList<EventType> eventTypeList = eventTypeClient.inNamespace("default").list();
    Assert.assertNotNull(eventTypeList);
    Assert.assertEquals(1, eventTypeList.getItems().size());
  }

  @Before
  public void setupEventTypeClient() {
    this.eventTypeClient = client.resources(EventType.class);
  }

}
