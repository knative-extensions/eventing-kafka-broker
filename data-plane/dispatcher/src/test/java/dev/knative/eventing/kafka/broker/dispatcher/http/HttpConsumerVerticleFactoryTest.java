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

package dev.knative.eventing.kafka.broker.dispatcher.http;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Empty;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.BackoffPolicy;
import dev.knative.eventing.kafka.broker.contract.DataPlaneContract.EgressConfig;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class HttpConsumerVerticleFactoryTest {

  @Test
  public void shouldAlwaysSucceed(final Vertx vertx) {

    final var consumerProperties = new Properties();
    consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
    consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());

    final var producerConfigs = new Properties();
    producerConfigs.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
    producerConfigs.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());

    final var verticleFactory = new HttpConsumerVerticleFactory(
      ConsumerRecordOffsetStrategyFactory.unordered(mock(Counter.class)),
      consumerProperties,
      WebClient.create(vertx),
      vertx,
      producerConfigs
    );

    final var egress = DataPlaneContract.Egress.newBuilder()
      .setConsumerGroup("1234")
      .setUid("1234")
      .setDestination("http://localhost:43256")
      .setReplyToOriginalTopic(Empty.newBuilder().build())
      .build();
    final var resource = DataPlaneContract.Resource.newBuilder()
      .setUid("123456")
      .setBootstrapServers("0.0.0.0:9092")
      .addTopics("t1")
      .setEgressConfig(DataPlaneContract.EgressConfig.newBuilder()
        .setBackoffDelay(1000)
        .setBackoffPolicy(BackoffPolicy.Exponential)
        .setRetry(10)
        .setDeadLetter("http://localhost:43257")
      )
      .addEgresses(egress)
      .build();

    assertDoesNotThrow(() -> verticleFactory.get(resource, egress));
  }

  @Test
  public void shouldNotThrowIllegalArgumentExceptionIfNotDLQ(final Vertx vertx) {

    final var consumerProperties = new Properties();
    consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
    consumerProperties
      .setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties
      .setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());

    final var producerConfigs = new Properties();
    producerConfigs.setProperty(BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
    producerConfigs.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs
      .setProperty(VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());

    final var verticleFactory = new HttpConsumerVerticleFactory(
      ConsumerRecordOffsetStrategyFactory.unordered(mock(Counter.class)),
      consumerProperties,
      WebClient.create(vertx),
      vertx,
      producerConfigs
    );

    final var egress = DataPlaneContract.Egress.newBuilder()
      .setConsumerGroup("1234")
      .setUid("1234")
      .setDestination("http://localhost:43256")
      .setReplyToOriginalTopic(Empty.newBuilder().build())
      .build();
    final var resource = DataPlaneContract.Resource.newBuilder()
      .setUid("123456")
      .setBootstrapServers("0.0.0.0:9092")
      .addTopics("t1")
      .addEgresses(egress)
      .build();

    assertDoesNotThrow(() -> verticleFactory.get(resource, egress));
  }

  @Test
  public void linearBackoffPolicy() {

    final var policy = HttpConsumerVerticleFactory.computeRetryPolicy(EgressConfig.newBuilder()
      .setRetry(10)
      .setBackoffPolicy(BackoffPolicy.Linear)
      .setBackoffDelay(100)
      .build());

    final var delay = policy.apply(5);

    assertThat(delay).isEqualTo(100 * 5);
  }

  @Test
  public void exponentialBackoffPolicy() {

    final var policy = HttpConsumerVerticleFactory.computeRetryPolicy(EgressConfig.newBuilder()
      .setRetry(10)
      .setBackoffPolicy(BackoffPolicy.Exponential)
      .setBackoffDelay(100)
      .build());

    final var delay = policy.apply(5);

    assertThat(delay).isEqualTo((long) (100 * Math.pow(2, 5)));
  }

  @Test
  public void exponentialBackoffPolicyByDefault() {

    final var policy = HttpConsumerVerticleFactory.computeRetryPolicy(EgressConfig.newBuilder()
      .setRetry(10)
      .setBackoffPolicy(BackoffPolicy.Exponential)
      .setBackoffDelay(100)
      .build());

    final var delay = policy.apply(5);

    assertThat(delay).isEqualTo((long) (100 * Math.pow(2, 5)));
  }

  @Test
  public void noRetry() {

    final var policy = HttpConsumerVerticleFactory.computeRetryPolicy(null);

    final var delay = policy.apply(Double.valueOf(Math.random()).intValue());

    assertThat(delay).isEqualTo(0);
  }
}
