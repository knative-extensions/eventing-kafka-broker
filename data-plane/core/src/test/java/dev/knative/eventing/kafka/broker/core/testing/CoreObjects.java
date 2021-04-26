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
package dev.knative.eventing.kafka.broker.core.testing;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;

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
      .addResources(resource1())
      .addResources(resource2())
      .build();
  }

  public static DataPlaneContract.Resource resource1() {
    return DataPlaneContract.Resource.newBuilder()
      .setUid("1-1234")
      .addTopics("1-12345")
      .addAllEgresses(Arrays.asList(
        egress1(),
        egress2()
      ))
      .build();
  }

  public static DataPlaneContract.Resource resource2() {
    return DataPlaneContract.Resource.newBuilder()
      .setUid("2-1234")
      .addTopics("2-12345")
      .addAllEgresses(Arrays.asList(
        egress3(),
        egress4()
      ))
      .build();
  }

  public static DataPlaneContract.Egress egress1() {
    return DataPlaneContract.Egress.newBuilder()
      .setUid("1-1234567")
      .setConsumerGroup("1-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .setEgressConfig(DataPlaneContract.EgressConfig.newBuilder()
        .setRetry(1)
        .setBackoffDelay(1000)
        .build())
      .build();
  }

  public static DataPlaneContract.Egress egress2() {
    return DataPlaneContract.Egress.newBuilder()
      .setUid("2-1234567")
      .setConsumerGroup("2-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  public static DataPlaneContract.Egress egress3() {
    return DataPlaneContract.Egress.newBuilder()
      .setUid("3-1234567")
      .setConsumerGroup("3-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  public static DataPlaneContract.Egress egress4() {
    return DataPlaneContract.Egress.newBuilder()
      .setUid("4-1234567")
      .setConsumerGroup("4-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  public static DataPlaneContract.Egress egress5() {
    return DataPlaneContract.Egress.newBuilder()
      .setUid("5-1234567")
      .setConsumerGroup("5-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  public static DataPlaneContract.Egress egress6() {
    return DataPlaneContract.Egress.newBuilder()
      .setUid("6-1234567")
      .setConsumerGroup("6-1234567")
      .setDestination(DESTINATION)
      .setFilter(DataPlaneContract.Filter.newBuilder().putAttributes("type", "dev.knative"))
      .build();
  }

  public static CloudEvent event() {
    return CloudEventBuilder.v1()
      .withId("abc")
      .withSource(URI.create("http://localhost"))
      .withType("test")
      .build();
  }

}
