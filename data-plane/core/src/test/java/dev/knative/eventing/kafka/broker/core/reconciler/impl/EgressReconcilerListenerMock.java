package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EgressReconcilerListenerMock implements EgressReconcilerListener {

  private final List<String> newEgresses;
  private final List<String> updatedEgresses;
  private final List<String> deletedEgresses;

  public EgressReconcilerListenerMock() {
    this.newEgresses = new ArrayList<>();
    this.updatedEgresses = new ArrayList<>();
    this.deletedEgresses = new ArrayList<>();
  }

  @Override
  public Future<Void> onNewEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(egress);
    this.newEgresses.add(resource.getUid());
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> onUpdateEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(egress);
    this.updatedEgresses.add(resource.getUid());
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> onDeleteEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(egress);
    this.deletedEgresses.add(resource.getUid());
    return Future.succeededFuture();
  }

  public List<String> getNewEgresses() {
    return newEgresses;
  }

  public List<String> getUpdatedEgresses() {
    return updatedEgresses;
  }

  public List<String> getDeletedEgresses() {
    return deletedEgresses;
  }
}
