package dev.knative.eventing.kafka.broker.core.eventtype;

import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;

import java.util.HashMap;
import java.util.Map;

public class EventTypeListerFactory {
  private final Map<String, Lister<EventType>> listerMap;
  private final SharedIndexInformer<EventType> eventTypeInformer;

  public EventTypeListerFactory(SharedIndexInformer<EventType> eventTypeInformer) {
    if (eventTypeInformer == null) {
      throw new IllegalArgumentException("you must provide a non null eventtype informer");
    }
    this.eventTypeInformer = eventTypeInformer;
    this.listerMap = new HashMap<>();
  }

  public Lister<EventType> getForNamespace(String namespace) {
    if (this.listerMap.containsKey(namespace)) {
      return this.listerMap.get(namespace);
    }
    return this.createListerForNamespace(namespace);
  }

  private Lister<EventType> createListerForNamespace(String namespace) {
    final var lister = new Lister<>(this.eventTypeInformer.getIndexer(), namespace);
    this.listerMap.put(namespace, lister);
    return lister;
  }
}
