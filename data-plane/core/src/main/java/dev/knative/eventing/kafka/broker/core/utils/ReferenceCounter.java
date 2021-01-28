package dev.knative.eventing.kafka.broker.core.utils;

import java.util.Objects;

/**
 * Thread unsafe holder with reference counter.
 *
 * @param <T> the type of the value to hold
 */
public class ReferenceCounter<T> {

  private final T value;
  private int refs;

  public ReferenceCounter(final T value) {
    Objects.requireNonNull(value);
    this.value = value;
    this.refs = 0;
  }

  /**
   * @return the inner value
   */
  public T getValue() {
    return value;
  }

  /**
   * Increment the ref count
   */
  public void increment() {
    this.refs++;
  }

  /**
   * @return true if the count is 0, hence nobody is referring anymore to this value
   */
  public boolean decrementAndCheck() {
    this.refs--;
    return this.refs == 0;
  }
}
