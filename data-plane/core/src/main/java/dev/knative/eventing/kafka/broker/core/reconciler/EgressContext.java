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

import java.util.Set;

public final class EgressContext {
  private final DataPlaneContract.Resource resource;

  private final DataPlaneContract.Egress egress;

  private final Set<String> trustBundles;

  EgressContext(DataPlaneContract.Resource resource, DataPlaneContract.Egress egress, Set<String> trustBundles) {
    this.resource = resource;
    this.egress = egress;
    this.trustBundles = trustBundles;
  }

  public DataPlaneContract.Resource resource() {
    return resource;
  }

  public DataPlaneContract.Egress egress() {
    return egress;
  }

  public Set<String> trustBundles() {
    return trustBundles;
  }
}
