package dev.knative.eventing.kafka.broker.core.utils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class CollectionsUtils {

  private CollectionsUtils() {
  }

  public static class DiffResult<T> {

    private final Set<T> added;
    private final Set<T> same;
    private final Set<T> removed;

    private DiffResult(Set<T> added, Set<T> same, Set<T> removed) {
      this.added = added;
      this.same = same;
      this.removed = removed;
    }

    public Set<T> getAdded() {
      return added;
    }

    public Set<T> getSame() {
      return same;
    }

    public Set<T> getRemoved() {
      return removed;
    }
  }

  public static <T> DiffResult<T> diff(Set<T> oldSet, Set<T> newSet) {
    Objects.requireNonNull(oldSet);
    Objects.requireNonNull(newSet);

    Set<T> added = new HashSet<>(newSet);
    added.removeAll(oldSet);

    Set<T> removed = new HashSet<>(oldSet);
    added.removeAll(newSet);

    Set<T> same = new HashSet<>(oldSet);
    same.retainAll(newSet);

    return new DiffResult<>(added, same, removed);
  }

}
