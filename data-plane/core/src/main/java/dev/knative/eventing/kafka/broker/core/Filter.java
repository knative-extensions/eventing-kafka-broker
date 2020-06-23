package dev.knative.eventing.kafka.broker.core;

/**
 * Filter interface abstract the filtering logic.
 *
 * @param <T> type of objects to filter.
 */
@FunctionalInterface
public interface Filter<T> {

  boolean match(final T event);
}
