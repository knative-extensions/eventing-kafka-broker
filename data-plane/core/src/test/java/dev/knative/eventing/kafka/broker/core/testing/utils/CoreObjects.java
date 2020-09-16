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

package dev.knative.eventing.kafka.broker.core.testing.utils;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.Egress;
import dev.knative.eventing.kafka.broker.core.EgressWrapper;
import dev.knative.eventing.kafka.broker.core.Resource;
import dev.knative.eventing.kafka.broker.core.ResourceWrapper;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class CoreObjects {

  public static URL DESTINATION_URL;

  static {
    try {
      DESTINATION_URL = new URL(
        "http", "localhost", 44331, ""
      );
    } catch (final MalformedURLException e) {
      e.printStackTrace();
    }
  }

  public static final String DESTINATION = DESTINATION_URL.toString();

  public static DataPlaneContract.Contract contract() {
    return DataPlaneContract.Contract.newBuilder()
      .addResources(resource1Unwrapped())
      .addResources(resource2Unwrapped())
      .build();
  }

  public static Resource resource1() {
    return new ResourceWrapper(
      resource1Unwrapped()
    );
  }

  public static DataPlaneContract.Resource resource1Unwrapped() {
    return DataPlaneContract.Resource.newBuilder()
      .setId("1-1234")
      .addTopics("1-12345")
      .addAllEgresses(Arrays.asList(
        egress11(),
        egress12()
      ))
      .build();
  }

  public static Resource resource2() {
    return new ResourceWrapper(
      resource2Unwrapped()
    );
  }

  public static DataPlaneContract.Resource resource2Unwrapped() {
    return DataPlaneContract.Resource.newBuilder()
      .setId("2-1234")
      .addTopics("2-12345")
      .addAllEgresses(Arrays.asList(
        egress13(),
        egress14()
      ))
      .build();
  }


  public static Egress egress1() {
    return new EgressWrapper(egress11());
  }

  public static Egress egress2() {
    return new EgressWrapper(egress12());
  }

  public static Egress egress3() {
    return new EgressWrapper(egress13());
  }

  public static Egress egress4() {
    return new EgressWrapper(egress14());
  }

  public static DataPlaneContract.Egress egress11() {
    return DataPlaneContract.Egress.newBuilder()
      .setConsumerGroup("1-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  public static DataPlaneContract.Egress egress12() {
    return DataPlaneContract.Egress.newBuilder()
      .setConsumerGroup("2-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  public static DataPlaneContract.Egress egress13() {
    return DataPlaneContract.Egress.newBuilder()
      .setConsumerGroup("3-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  public static DataPlaneContract.Egress egress14() {
    return DataPlaneContract.Egress.newBuilder()
      .setConsumerGroup("4-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  /**
   * This method generates a collection of resource mocked with new egresses, so you can use it to test the {@link dev.knative.eventing.kafka.broker.core.ObjectsReconciler}
   */
  public static Collection<Resource> mockResourcesWithNewEgresses(Map<Resource, Set<Egress>> newResources) {
    return newResources.entrySet().stream().map(entry -> new Resource() {
      @Override
      public String id() {
        return entry.getKey().id();
      }

      @Override
      public Set<String> topics() {
        return entry.getKey().topics();
      }

      @Override
      public String bootstrapServers() {
        return entry.getKey().bootstrapServers();
      }

      @Override
      public DataPlaneContract.Ingress ingress() {
        return entry.getKey().ingress();
      }

      @Override
      public List<Egress> egresses() {
        return new ArrayList<>(entry.getValue());
      }

      @Override
      public DataPlaneContract.EgressConfig egressConfig() {
        return entry.getKey().egressConfig();
      }
    }).collect(Collectors.toList());
  }
}
