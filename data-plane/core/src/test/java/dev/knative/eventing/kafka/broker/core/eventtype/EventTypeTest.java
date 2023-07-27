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
