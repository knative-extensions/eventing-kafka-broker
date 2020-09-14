package dev.knative.eventing.kafka.broker.core;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import java.util.Objects;

public class IngressWrapper implements Ingress {

  final DataPlaneContract.Ingress ingress;

  public IngressWrapper(DataPlaneContract.Ingress ingress) {
    this.ingress = ingress;
  }

  @Override
  public boolean isPathType() {
    return ingress.getIngressTypeCase() == DataPlaneContract.Ingress.IngressTypeCase.PATH;
  }

  @Override
  public String path() {
    return ingress.getPath();
  }

  @Override
  public boolean isHostType() {
    return ingress.getIngressTypeCase() == DataPlaneContract.Ingress.IngressTypeCase.HOST;
  }

  @Override
  public String host() {
    return ingress.getHost();
  }

  @Override
  public DataPlaneContract.ContentMode contentMode() {
    return ingress.getContentMode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IngressWrapper that = (IngressWrapper) o;
    return Objects.equals(this.isPathType(), that.isPathType())
      && Objects.equals(this.path(), that.path())
      && Objects.equals(this.isHostType(), that.isHostType())
      && Objects.equals(this.host(), that.host())
      && Objects.equals(this.contentMode(), that.contentMode());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      isPathType(),
      path(),
      isHostType(),
      host(),
      contentMode()
    );
  }

  @Override
  public String toString() {
    return "IngressWrapper{" +
      "ingress=" + ingress +
      '}';
  }
}
