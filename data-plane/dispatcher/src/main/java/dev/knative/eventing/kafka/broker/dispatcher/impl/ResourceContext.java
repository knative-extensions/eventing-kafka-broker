package dev.knative.eventing.kafka.broker.dispatcher.impl;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.metrics.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public class ResourceContext {

  private final DataPlaneContract.Resource resource;
  private final DataPlaneContract.Egress egress;
  private final Tags tags;

  public ResourceContext(DataPlaneContract.Resource resource, DataPlaneContract.Egress egress) {
    this.resource = resource;
    this.egress = egress;

    this.tags = Tags.of(
      // Resource tags
      Tag.of(Metrics.Tags.RESOURCE_NAME, resource.getReference().getName()),
      Tag.of(Metrics.Tags.RESOURCE_NAMESPACE, resource.getReference().getNamespace()),
      // Egress tags
      Tag.of(Metrics.Tags.CONSUMER_NAME, egress.getReference().getName())
    );
  }

  public DataPlaneContract.Resource getResource() {
    return resource;
  }

  public DataPlaneContract.Egress getEgress() {
    return egress;
  }

  public Tags getTags() {
    return tags;
  }
}
