package dev.knative.eventing.kafka.broker.core;

import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import java.util.Objects;

/**
 * BrokerWrapper wraps a Broker for implementing the Broker interface.
 *
 * <p>The wrapped Broker Broker must not be modified by callers.
 */
public class BrokerWrapper implements dev.knative.eventing.kafka.broker.core.Broker {

  private final Broker broker;

  /**
   * All args constructor.
   *
   * @param broker broker (it must not be modified by callers)
   */
  public BrokerWrapper(final Broker broker) {
    this.broker = broker;
  }

  @Override
  public String id() {
    return broker.getId();
  }

  @Override
  public String topic() {
    return broker.getTopic();
  }

  @Override
  public String deadLetterSink() {
    return broker.getDeadLetterSink();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrokerWrapper that = (BrokerWrapper) o;

    return broker.getId().equals(that.id())
        && broker.getDeadLetterSink().equals(that.deadLetterSink())
        && broker.getTopic().equals(that.topic());
  }

  @Override
  public int hashCode() {
    return Objects.hash(broker.getId(), broker.getDeadLetterSink(), broker.getTopic());
  }
}
