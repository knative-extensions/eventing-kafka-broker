package dev.knative.eventing.kafka.broker.receiver;

import static dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper.TOPIC_PREFIX;
import static dev.knative.eventing.kafka.broker.receiver.CloudEventRequestToRecordMapper.topic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class CloudEventRequestToRecordMapperTest {

  @Test
  public void shouldReturnEmptyTopicIfRootPath() {

    assertNull(topic("/"));

    assertNull(topic(""));
  }

  @Test
  public void shouldReturnNullIfNoBrokerName() {

    assertThat(topic("/broker-namespace")).isNull();

    assertThat(topic("/broker-namespace/")).isNull();
  }

  @Test
  public void shouldReturnTopicNameIfCorrectPath() {

    assertThat(topic("/broker-namespace/broker-name"))
        .isEqualTo(TOPIC_PREFIX + "broker-namespace-broker-name");

    assertThat(topic("/broker-namespace/broker-name/"))
        .isEqualTo(TOPIC_PREFIX + "broker-namespace-broker-name");
  }

  @Test
  public void shouldReturnEmptyIfBrokerNamespaceAndBrokerNameAreFollowedBySomething() {

    assertThat(topic("/broker-namespace/broker-name/something")).isNull();

    assertThat(topic("/broker-namespace/broker-name/something/")).isNull();
  }
}