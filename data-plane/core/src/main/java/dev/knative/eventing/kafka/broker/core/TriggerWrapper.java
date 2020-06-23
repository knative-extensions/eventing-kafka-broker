package dev.knative.eventing.kafka.broker.core;

import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Trigger;
import io.cloudevents.CloudEvent;
import java.util.Map;
import java.util.Objects;

/**
 * TriggerWrapper wraps a Trigger for implementing the Trigger interface.
 *
 * <p>The wrapped Trigger Trigger must not be modified by callers.
 */
public class TriggerWrapper implements dev.knative.eventing.kafka.broker.core.Trigger<CloudEvent> {

  private final Trigger trigger;

  /**
   * All args constructor.
   *
   * @param trigger trigger (it must not be modified by callers)
   */
  public TriggerWrapper(final Trigger trigger) {
    this.trigger = trigger;
  }

  @Override
  public String id() {
    return trigger.getId();
  }

  @Override
  public Filter<CloudEvent> filter() {
    return new EventMatcher(trigger.getAttributesMap());
  }

  @Override
  public String destination() {
    return trigger.getDestination();
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof TriggerWrapper)) {
      return false;
    }
    final var t = (TriggerWrapper) object;
    return t.trigger.getId().equals(trigger.getId())
        && t.trigger.getDestination().equals(trigger.getDestination())
        && mapEquals(t.trigger.getAttributesMap(), trigger.getAttributesMap());
  }

  @Override
  public int hashCode() {
    final var hashAttributes = trigger.getAttributesMap().entrySet().stream()
        .mapToInt(entry -> Objects.hash(entry.getKey(), entry.getValue()))
        .sum();

    return Objects.hash(
        trigger.getId(),
        trigger.getDestination(),
        hashAttributes
    );
  }

  // TODO re-evaluate hashcode and equals
  private static boolean mapEquals(final Map<String, String> m1, final Map<String, String> m2) {
    final var count = m1.entrySet().stream()
        .map(entry -> m2.containsKey(entry.getKey())
            && m2.get(entry.getKey()).equals(entry.getValue()))
        .filter(Boolean::booleanValue)
        .count();
    return count == m1.size() && count == m2.size();
  }

  @Override
  public String toString() {
    return "TriggerWrapper{"
        + "trigger=" + trigger
        + '}';
  }
}
