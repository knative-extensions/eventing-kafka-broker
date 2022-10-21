/*
 * Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
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
package dev.knative.eventing.kafka.broker.dispatcher.main;

import io.vertx.core.Vertx;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.kafka.client.common.KafkaClientOptions;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.Map;
import java.util.Properties;

@FunctionalInterface
public interface ProducerFactory<K, V> {

  KafkaProducer<K, V> create(final Vertx vertx, final Map<String, Object> configs);

  static <K, V> ProducerFactory<K, V> defaultFactory() {
    return new ProducerFactory<>() {

      private KafkaProducer<K, V> producer;

      @Override
      public KafkaProducer<K, V> create(Vertx vertx, Map<String, Object> configs) {
        if (producer == null) {
          Properties producerProperties = new Properties();
          producerProperties.putAll(configs);
          producer = KafkaProducer.create(vertx, producerProperties);
        }
        return producer;
      }
    };
  }
}
