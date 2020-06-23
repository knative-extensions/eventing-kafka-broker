package dev.knative.eventing.kafka.broker.core;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Trigger;
import io.cloudevents.CloudEvent;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Execution(value = ExecutionMode.CONCURRENT)
public class TriggerWrapperTest {

  @ParameterizedTest
  @MethodSource(value = {"equalTriggersProvider"})
  public void testTriggerEquality(final TriggerWrapper t1, final TriggerWrapper t2) {
    assertThat(t1).isEqualTo(t2);
    assertThat(t1.hashCode()).isEqualTo(t2.hashCode());
  }

  @ParameterizedTest
  @MethodSource(value = {"differentTriggersProvider"})
  public void testTriggerDifference(final TriggerWrapper t1, final TriggerWrapper t2) {
    assertThat(t1).isNotEqualTo(t2);
    assertThat(t1.hashCode()).isNotEqualTo(t2.hashCode());
  }

  @Test
  public void idCallShouldBeDelegatedToWrappedTrigger() {
    final var id = "123-42";
    final var triggerWrapper = new TriggerWrapper(
        Trigger.newBuilder().setId(id).build()
    );

    assertThat(triggerWrapper.id()).isEqualTo(id);

  }

  @Test
  public void destinationCallShouldBeDelegatedToWrappedTrigger() {
    final var destination = "destination-42";
    final var triggerWrapper = new TriggerWrapper(
        Trigger.newBuilder().setDestination(destination).build()
    );

    assertThat(triggerWrapper.destination()).isEqualTo(destination);
  }

  // test if filter returned by filter() agrees with EventMatcher
  @ParameterizedTest
  @MethodSource(value = "dev.knative.eventing.kafka.broker.core.EventMatcherTest#testCases")
  public void testFilter(
      final Map<String, String> attributes,
      final CloudEvent event,
      final boolean shouldMatch) {
    final var triggerWrapper = new TriggerWrapper(
        Trigger.newBuilder()
            .putAllAttributes(attributes)
            .build()
    );

    final var filter = triggerWrapper.filter();

    final var match = filter.match(event);

    assertThat(match).isEqualTo(shouldMatch);
  }

  public static Stream<Arguments> differentTriggersProvider() {
    return Stream.of(
        // trigger's destination is different
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Collections.emptyMap())
                .setDestination("this-is-my-destination1")
                .setId("1234-hello")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Collections.emptyMap())
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        ),
        // trigger's attributes are different
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion1",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        ),
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value1"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        ),
        // trigger's id is different
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello1")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        )
    );
  }

  public static Stream<Arguments> equalTriggersProvider() {
    return Stream.of(
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type_value"
                ))
                .setDestination("this-is-my-destination")
                .setId("1234-hello")
                .build()
            )
        ),
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .build()
            )
        ),
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .setId("1234")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .setId("1234")
                .build()
            )
        ),
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .build()
            )
        ),
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .setDestination("dest")
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .setDestination("dest")
                .build()
            )
        ),
        Arguments.of(
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type1"
                ))
                .build()
            ),
            new TriggerWrapper(Trigger
                .newBuilder()
                .putAllAttributes(Map.of(
                    "specversion",
                    "1.0",
                    "type",
                    "type1"
                ))
                .build()
            )
        )
    );
  }
}