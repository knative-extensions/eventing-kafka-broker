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
