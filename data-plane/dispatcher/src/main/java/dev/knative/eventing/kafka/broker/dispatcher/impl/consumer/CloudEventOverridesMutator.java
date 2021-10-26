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
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventMutator;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

/**
 * CloudEventOverridesMutator is a {@link CloudEventMutator} that applies a given set of
 * {@link dev.knative.eventing.kafka.broker.contract.DataPlaneContract.CloudEventOverrides}.
 */
public class CloudEventOverridesMutator implements CloudEventMutator {

  private final DataPlaneContract.CloudEventOverrides cloudEventOverrides;

  public CloudEventOverridesMutator(final DataPlaneContract.CloudEventOverrides cloudEventOverrides) {
    this.cloudEventOverrides = cloudEventOverrides;
  }

  @Override
  public CloudEvent apply(CloudEvent cloudEvent) {
    if (cloudEventOverrides.getExtensionsMap().isEmpty()) {
      return cloudEvent;
    }
    final var builder = CloudEventBuilder.from(cloudEvent);
    applyCloudEventOverrides(builder);
    return builder.build();
  }

  private void applyCloudEventOverrides(CloudEventBuilder builder) {
    cloudEventOverrides.getExtensionsMap()
      .forEach(builder::withExtension);
  }
}
