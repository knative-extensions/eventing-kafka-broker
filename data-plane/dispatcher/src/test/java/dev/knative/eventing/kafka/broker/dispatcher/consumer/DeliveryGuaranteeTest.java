package dev.knative.eventing.kafka.broker.dispatcher.consumer;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeliveryGuaranteeTest {

  @Test
  void fromContract() {
    assertThat(DeliveryGuarantee.fromContract(null))
      .isEqualTo(DeliveryGuarantee.UNORDERED);

    assertThat(DeliveryGuarantee.fromContract(DataPlaneContract.DeliveryGuarantee.UNORDERED))
      .isEqualTo(DeliveryGuarantee.UNORDERED);
    assertThat(DeliveryGuarantee.fromContract(DataPlaneContract.DeliveryGuarantee.UNRECOGNIZED))
      .isEqualTo(DeliveryGuarantee.UNORDERED);

    assertThat(DeliveryGuarantee.fromContract(DataPlaneContract.DeliveryGuarantee.ORDERED))
      .isEqualTo(DeliveryGuarantee.ORDERED);
  }
}
