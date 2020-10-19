package dev.knative.eventing.kafka.broker.core.reconciler;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.vertx.core.Future;

public interface IngressReconcilerListener {

  Future<Void> onNewIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress);

  Future<Void> onUpdateIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress);

  Future<Void> onDeleteIngress(DataPlaneContract.Resource resource, DataPlaneContract.Ingress ingress);

}
