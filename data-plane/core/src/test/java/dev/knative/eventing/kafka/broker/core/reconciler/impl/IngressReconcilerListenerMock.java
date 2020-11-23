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
