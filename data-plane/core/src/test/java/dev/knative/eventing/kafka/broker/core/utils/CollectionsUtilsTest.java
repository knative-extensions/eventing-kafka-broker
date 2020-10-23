package dev.knative.eventing.kafka.broker.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class CollectionsUtilsTest {

  @Test
  void diff() {
    Set<String> oldSet = Set.of("aaa", "bbb", "ccc");
    Set<String> newSet = Set.of("ccc", "ddd", "eee");

    final CollectionsUtils.DiffResult<String> diff = CollectionsUtils.diff(oldSet, newSet);
    assertThat(diff.getAdded())
      .containsExactlyInAnyOrder("ddd", "eee");
    assertThat(diff.getIntersection())
      .containsExactlyInAnyOrder("ccc");
    assertThat(diff.getRemoved())
      .containsExactlyInAnyOrder("aaa", "bbb");
  }
}
