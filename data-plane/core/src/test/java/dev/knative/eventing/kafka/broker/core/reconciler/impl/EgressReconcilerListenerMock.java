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
  private final Future<Void> onNewEgressFuture;
  private final Future<Void> onUpdateEgressFuture;
  private final Future<Void> onDeleteDeleteFuture;

  public EgressReconcilerListenerMock() {
    this(Future.succeededFuture(), Future.succeededFuture(), Future.succeededFuture());
  }

  public EgressReconcilerListenerMock(Future<Void> onNewEgressFuture, Future<Void> onUpdateEgressFuture, Future<Void> onDeleteDeleteFuture) {
    this.onNewEgressFuture = onNewEgressFuture;
    this.onUpdateEgressFuture = onUpdateEgressFuture;
    this.onDeleteDeleteFuture = onDeleteDeleteFuture;
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
    this.newEgresses.add(egress.getUid());
    return this.onNewEgressFuture;
  }

  @Override
  public Future<Void> onUpdateEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(egress);
    this.updatedEgresses.add(egress.getUid());
    return this.onUpdateEgressFuture;
  }

  @Override
  public Future<Void> onDeleteEgress(
    DataPlaneContract.Resource resource,
    DataPlaneContract.Egress egress) {
    Objects.requireNonNull(resource);
    Objects.requireNonNull(egress);
    this.deletedEgresses.add(egress.getUid());
    return this.onDeleteDeleteFuture;
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
