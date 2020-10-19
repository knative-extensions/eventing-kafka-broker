package dev.knative.eventing.kafka.broker.core.reconciler;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.vertx.core.Future;

public interface EgressReconcilerListener {

  Future<Void> onNewEgress(DataPlaneContract.Resource resource, DataPlaneContract.Egress egress);

  Future<Void> onUpdateEgress(DataPlaneContract.Resource resource, DataPlaneContract.Egress egress);

  Future<Void> onDeleteEgress(DataPlaneContract.Resource resource, DataPlaneContract.Egress egress);

}
