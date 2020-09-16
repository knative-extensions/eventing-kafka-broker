package dev.knative.eventing.kafka.broker.core;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.Objects;

public class EgressConfigWrapper implements EgressConfig {

  private final DataPlaneContract.EgressConfig egressConfig;

  public EgressConfigWrapper(DataPlaneContract.EgressConfig egressConfig) {
    this.egressConfig = egressConfig;
  }

  @Override
  public String deadLetter() {
    return egressConfig.getDeadLetter();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EgressConfigWrapper that = (EgressConfigWrapper) o;
    return Objects.equals(deadLetter(), that.deadLetter());
  }

  @Override
  public int hashCode() {
    return Objects.hash(deadLetter());
  }
}
