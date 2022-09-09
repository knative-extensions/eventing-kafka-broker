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
package dev.knative.eventing.kafka.broker.core.reconciler;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public interface IngressReconcilerListener {

  Future<Void> onNewIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress);

  Future<Void> onUpdateIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress);

  Future<Void> onDeleteIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress);

  static IngressReconcilerListener all(final IngressReconcilerListener... listeners) {
    return new CompositeIngressReconcilerLister(listeners);
  }
}

class CompositeIngressReconcilerLister implements IngressReconcilerListener {

  private IngressReconcilerListener[] listeners;

  public CompositeIngressReconcilerLister(final IngressReconcilerListener... listeners) {
    Objects.requireNonNull(listeners);
    this.listeners = listeners;
  }

  @Override
  public Future<Void> onNewIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress) {
    return CompositeFuture.all(
      Arrays.stream(listeners)
        .map(l -> l.onNewIngress(resource, ingress))
        .collect(Collectors.toList())
    ).mapEmpty();
  }

  @Override
  public Future<Void> onUpdateIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress) {
    return CompositeFuture.all(
      Arrays.stream(listeners)
        .map(l -> l.onUpdateIngress(resource, ingress))
        .collect(Collectors.toList())
    ).mapEmpty();
  }

  @Override
  public Future<Void> onDeleteIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress) {
    return CompositeFuture.all(
      Arrays.stream(listeners)
        .map(l -> l.onDeleteIngress(resource, ingress))
        .collect(Collectors.toList())
    ).mapEmpty();
  }
}
