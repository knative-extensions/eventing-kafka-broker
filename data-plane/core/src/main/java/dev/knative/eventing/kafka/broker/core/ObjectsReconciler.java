package dev.knative.eventing.kafka.broker.core;

import io.vertx.core.Future;
import java.util.Map;
import java.util.Set;

@FunctionalInterface
public interface ObjectsReconciler<T> {

  Future<Void> reconcile(Map<Broker, Set<Trigger<T>>> objects) throws Exception;
}
