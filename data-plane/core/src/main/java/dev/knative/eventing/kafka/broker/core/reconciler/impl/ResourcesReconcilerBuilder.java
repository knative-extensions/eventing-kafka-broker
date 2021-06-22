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

import dev.knative.eventing.kafka.broker.core.reconciler.EgressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.IngressReconcilerListener;
import dev.knative.eventing.kafka.broker.core.reconciler.ResourcesReconciler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import java.util.Objects;

public class ResourcesReconcilerBuilder {

  private IngressReconcilerListener ingressReconcilerListener;
  private EgressReconcilerListener egressReconcilerListener;

  public ResourcesReconcilerBuilder watchIngress(
    IngressReconcilerListener ingressReconcilerListener) {
    Objects.requireNonNull(ingressReconcilerListener);
    this.ingressReconcilerListener = ingressReconcilerListener;
    return this;
  }

  public ResourcesReconcilerBuilder watchEgress(
    EgressReconcilerListener egressReconcilerListener) {
    Objects.requireNonNull(egressReconcilerListener);
    this.egressReconcilerListener = egressReconcilerListener;
    return this;
  }

  /**
   * Build the {@link ResourcesReconciler} and start listening for new contracts using {@link ResourcesReconcilerMessageHandler}.
   *
   * @param vertx the vertx object to register the event bus listener
   * @return the built message consumer
   */
  public MessageConsumer<Object> buildAndListen(Vertx vertx) {
    return ResourcesReconcilerMessageHandler.start(vertx, build());
  }

  /**
   * @return the built {@link ResourcesReconciler}
   */
  public ResourcesReconciler build() {
    return new ResourcesReconcilerImpl(ingressReconcilerListener, egressReconcilerListener);
  }
}
