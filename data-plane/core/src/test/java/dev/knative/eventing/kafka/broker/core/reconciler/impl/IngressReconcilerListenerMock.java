package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IngressReconcilerListenerMock implements IngressReconcilerListener {

  private final List<String> newIngresses;
  private final List<String> updatedIngresses;
  private final List<String> deletedIngresses;

  public IngressReconcilerListenerMock() {
    this.newIngresses = new ArrayList<>();
    this.updatedIngresses = new ArrayList<>();
    this.deletedIngresses = new ArrayList<>();
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

  public List<String> getNewIngresses() {
    return newIngresses;
  }

  public List<String> getUpdatedIngresses() {
    return updatedIngresses;
  }

  public List<String> getDeletedIngresses() {
    return deletedIngresses;
  }
}
