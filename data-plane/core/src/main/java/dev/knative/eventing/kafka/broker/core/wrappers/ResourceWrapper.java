/*
 * Copyright 2020 The Knative Authors
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

package dev.knative.eventing.kafka.broker.core.wrappers;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ResourceWrapper wraps a Resource for implementing the Resource interface.
 *
 * <p>The wrapped Resource Resource must not be modified by callers.
 */
public class ResourceWrapper implements Resource {

  private final DataPlaneContract.Resource resource;

  /**
   * All args constructor.
   *
   * @param resource resource (it must not be modified by callers)
   */
  public ResourceWrapper(final DataPlaneContract.Resource resource) {
    this.resource = resource;
  }

  @Override
  public String id() {
    return resource.getUid();
  }

  @Override
  public Set<String> topics() {
    return IntStream.range(0, resource.getTopicsCount())
      .mapToObj(resource::getTopics)
      .collect(Collectors.toSet());
  }

  @Override
  public String bootstrapServers() {
    return resource.getBootstrapServers();
  }

  @Override
  public DataPlaneContract.Ingress ingress() {
    return resource.getIngress();
  }

  @Override
  public DataPlaneContract.EgressConfig egressConfig() {
    return resource.getEgressConfig();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResourceWrapper that = (ResourceWrapper) o;

    return Objects.equals(this.id(), that.id())
      && Objects.equals(this.topics(), that.topics())
      && Objects.equals(this.bootstrapServers(), that.bootstrapServers())
      && Objects.equals(this.ingress(), that.ingress())
      && Objects.equals(this.egressConfig(), that.egressConfig());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      this.id(),
      this.topics(),
      this.bootstrapServers(),
      this.ingress(),
      this.egressConfig()
    );
  }

  @Override
  public String toString() {
    return "ResourceWrapper{"
      + "resource=" + resource
      + '}';
  }
}
