/*
 * Copyright 2020 The Knative Authors
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

package dev.knative.eventing.kafka.broker.core.utils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class CollectionsUtils {

  private CollectionsUtils() {
  }

  public static class DiffResult<T> {

    private final Set<T> added;
    private final Set<T> intersection;
    private final Set<T> removed;

    private DiffResult(Set<T> added, Set<T> intersection, Set<T> removed) {
      this.added = added;
      this.intersection = intersection;
      this.removed = removed;
    }

    public Set<T> getAdded() {
      return added;
    }

    public Set<T> getIntersection() {
      return intersection;
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
    removed.removeAll(newSet);

    Set<T> intersection = new HashSet<>(oldSet);
    intersection.retainAll(newSet);

    return new DiffResult<>(added, intersection, removed);
  }

}
