package dev.knative.eventing.kafka.broker.core;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;

//TODO docs
public interface Ingress {

  boolean isPathType();

  /**
   * Get request path to accept events for this Resource.
   *
   * @return request path associated with this Resource.
   */
  String path();

  boolean isHostType();

  String host();

  /**
   * Get content mode.
   *
   * @return content mode.
   */
  DataPlaneContract.ContentMode contentMode();

}
