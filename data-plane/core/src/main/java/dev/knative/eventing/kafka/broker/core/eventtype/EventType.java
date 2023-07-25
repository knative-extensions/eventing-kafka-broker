package dev.knative.eventing.kafka.broker.core.eventtype;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.fabric8.kubernetes.model.annotation.Kind;

@Group("eventing.knative.dev")
@Version("v1beta2")
@Kind("EventType")
public class EventType extends CustomResource<EventTypeSpec, EventTypeStatus> implements Namespaced { }
