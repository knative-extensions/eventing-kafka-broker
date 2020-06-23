package dev.knative.eventing.kafka.broker.core;

import static org.assertj.core.api.Assertions.assertThat;

import dev.knative.eventing.kafka.broker.core.config.BrokersConfig.Broker;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BrokerWrapperTest {

  @Test
  public void idCallShouldBeDelegatedToWrappedBroker() {
    final var id = "123-42";
    final var broker = new BrokerWrapper(
        Broker.newBuilder().setId(id).build()
    );

    assertThat(broker.id()).isEqualTo(id);
  }

  @Test
  public void deadLetterSinkCallShouldBeDelegatedToWrappedBroker() {
    final var deadLetterSink = "http://localhost:9090/api";
    final var broker = new BrokerWrapper(
        Broker.newBuilder().setDeadLetterSink(deadLetterSink).build()
    );

    assertThat(broker.deadLetterSink()).isEqualTo(deadLetterSink);
  }

  @Test
  public void topicCallShouldBeDelegatedToWrappedBroker() {
    final var topic = "knative-topic";
    final var broker = new BrokerWrapper(
        Broker.newBuilder().setTopic(topic).build()
    );

    assertThat(broker.topic()).isEqualTo(topic);
  }

  @ParameterizedTest
  @MethodSource(value = {"equalTriggersProvider"})
  public void testTriggerEquality(
      final dev.knative.eventing.kafka.broker.core.Broker b1,
      final dev.knative.eventing.kafka.broker.core.Broker b2) {
   
    assertThat(b1).isEqualTo(b2);
    assertThat(b1.hashCode()).isEqualTo(b2.hashCode());
  }

  @ParameterizedTest
  @MethodSource(value = {"differentTriggersProvider"})
  public void testTriggerDifference(
      final dev.knative.eventing.kafka.broker.core.Broker b1,
      final dev.knative.eventing.kafka.broker.core.Broker b2) {

    assertThat(b1).isNotEqualTo(b2);
    assertThat(b1.hashCode()).isNotEqualTo(b2.hashCode());
  }

  public static Stream<Arguments> differentTriggersProvider() {
    return Stream.of(
        Arguments.of(
            new BrokerWrapper(
                Broker.newBuilder()
                    .setId("1234-id")
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder().build()
            )
        ),
        Arguments.of(
            new BrokerWrapper(
                Broker.newBuilder()
                    .setTopic("kantive-topic")
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder().build()
            )
        ),
        Arguments.of(
            new BrokerWrapper(
                Broker.newBuilder()
                    .setDeadLetterSink("http:/localhost:9090")
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder().build()
            )
        )
    );
  }

  public static Stream<Arguments> equalTriggersProvider() {
    return Stream.of(
        Arguments.of(
            new BrokerWrapper(
                Broker.newBuilder()
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder().build()
            )
        ),
        Arguments.of(
            new BrokerWrapper(
                Broker.newBuilder()
                    .setTopic("knative-topic")
                    .setId("1234-42")
                    .setDeadLetterSink("http://localhost:9090")
                    .build()
            ),
            new BrokerWrapper(
                Broker.newBuilder()
                    .setId("1234-42")
                    .setTopic("knative-topic")
                    .setDeadLetterSink("http://localhost:9090")
                    .build()
            )
        )
    );
  }
}