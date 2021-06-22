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
