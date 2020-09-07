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

import dev.knative.eventing.kafka.broker.core.Broker;
import dev.knative.eventing.kafka.broker.core.EventMatcher;
import dev.knative.eventing.kafka.broker.core.Filter;
import dev.knative.eventing.kafka.broker.core.Trigger;
import dev.knative.eventing.kafka.broker.dispatcher.ConsumerRecordOffsetStrategyFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import java.util.HashMap;
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
      ConsumerRecordOffsetStrategyFactory.unordered(),
      consumerProperties,
      WebClient.create(vertx),
      vertx,
      producerConfigs
    );

    final var consumerFactoryFuture = verticleFactory.get(
      new Broker() {
        @Override
        public String id() {
          return "123456";
        }

        @Override
        public String topic() {
          return "t1";
        }

        @Override
        public String deadLetterSink() {
          return "http://localhost:43257";
        }

        @Override
        public String bootstrapServers() {
          return "0.0.0.0:9092";
        }

        @Override
        public String path() {
          return null;
        }
      },
      new Trigger<>() {
        @Override
        public String id() {
          return "1234";
        }

        @Override
        public Filter<CloudEvent> filter() {
          return new EventMatcher(new HashMap<>());
        }

        @Override
        public String destination() {
          return "http://localhost:43256";
        }
      }
    );

    assertThat(consumerFactoryFuture.succeeded()).isTrue();
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
      ConsumerRecordOffsetStrategyFactory.unordered(),
      consumerProperties,
      WebClient.create(vertx),
      vertx,
      producerConfigs
    );

    assertDoesNotThrow(() -> {
      verticleFactory.get(
        new Broker() {
          @Override
          public String id() {
            return "123456";
          }

          @Override
          public String topic() {
            return "t1";
          }

          @Override
          public String deadLetterSink() {
            return "";
          }

          @Override
          public String bootstrapServers() {
            return "0.0.0.0:9092";
          }

          @Override
          public String path() {
            return null;
          }
        },
        new Trigger<>() {
          @Override
          public String id() {
            return "1234";
          }

          @Override
          public Filter<CloudEvent> filter() {
            return new EventMatcher(new HashMap<>());
          }

          @Override
          public String destination() {
            return "http://localhost:43256";
          }
        });
    });
  }
}
