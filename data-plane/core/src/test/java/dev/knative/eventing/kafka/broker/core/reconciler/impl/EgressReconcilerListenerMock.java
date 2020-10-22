package dev.knative.eventing.kafka.broker.core.reconciler.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import io.vertx.core.Future;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class EgressReconcilerListenerMock implements EgressReconcilerListener {

  private final Set<String> newEgresses;
  private final Set<String> updatedEgresses;
  private final Set<String> deletedEgresses;

  public EgressReconcilerListenerMock() {
    this.newEgresses = new HashSet<>();
    this.updatedEgresses = new HashSet<>();
    this.deletedEgresses = new HashSet<>();
  }

  @Override
  public Future<Void> onNewEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(egress);
    this.newEgresses.add(egress.getUid());
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> onUpdateEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(egress);
    this.updatedEgresses.add(egress.getUid());
    return Future.succeededFuture();
  }

  @Override
  public Future<Void> onDeleteEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(egress);
    this.deletedEgresses.add(egress.getUid());
    return Future.succeededFuture();
  }

  public Set<String> getNewEgresses() {
    return newEgresses;
  }

  public Set<String> getUpdatedEgresses() {
    return updatedEgresses;
  }

  public Set<String> getDeletedEgresses() {
    return deletedEgresses;
  }
}
