package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import io.vertx.core.Future;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class IngressReconcilerListenerMock implements IngressReconcilerListener {

  private final Set<String> newIngresses;
  private final Set<String> updatedIngresses;
  private final Set<String> deletedIngresses;

  public IngressReconcilerListenerMock() {
    this.newIngresses = new HashSet<>();
    this.updatedIngresses = new HashSet<>();
    this.deletedIngresses = new HashSet<>();
  }

  @Override
  public Future<Void> onNewIngress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Ingress ingress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(ingress);
    this.newIngresses.add(resource.getUid());
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> onUpdateIngress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Ingress ingress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(ingress);
    this.updatedIngresses.add(resource.getUid());
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> onDeleteIngress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Ingress ingress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(ingress);
    this.deletedIngresses.add(resource.getUid());
    return Future.succeededFuture();
  }

  public Set<String> getNewIngresses() {
    return newIngresses;
  }

  public Set<String> getUpdatedIngresses() {
    return updatedIngresses;
  }

  public Set<String> getDeletedIngresses() {
    return deletedIngresses;
  }
}
