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
package dev.knative.eventing.kafka.broker.dispatcher.consumer;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeliveryOrderTest {

  @Test
  void fromContract() {
    assertThat(DeliveryOrder.fromContract(null))
      .isEqualTo(DeliveryOrder.UNORDERED);

    assertThat(DeliveryOrder.fromContract(DataPlaneContract.DeliveryOrder.UNORDERED))
      .isEqualTo(DeliveryOrder.UNORDERED);
    assertThat(DeliveryOrder.fromContract(DataPlaneContract.DeliveryOrder.UNRECOGNIZED))
      .isEqualTo(DeliveryOrder.UNORDERED);

    assertThat(DeliveryOrder.fromContract(DataPlaneContract.DeliveryOrder.ORDERED))
      .isEqualTo(DeliveryOrder.ORDERED);
  }
}
